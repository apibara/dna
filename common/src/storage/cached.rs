use std::{num::NonZeroUsize, sync::Arc};

use error_stack::ResultExt;
use lru::LruCache;
use memmap2::Mmap;
use tokio::{
    io::AsyncWriteExt,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};
use tonic::async_trait;

use crate::error::{DnaError, Result};

use super::{AppStorageBackend, LocalStorageBackend, StorageBackend};

pub struct CacheOptions {
    pub prefix: String,
    pub max_file_count: usize,
    pub max_size_bytes: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct FileID {
    prefix: String,
    filename: String,
}

type FileLruCache = LruCache<FileID, u64>;

#[derive(Clone)]
struct CacheState {
    pub prefix: String,
    pub max_size_bytes: u64,
    pub mutable: Arc<RwLock<MutableCacheState>>,
}

/// Mutable component of the cache.
///
/// Since the cache is shared between multiple tasks, it needs to be protected by a lock.
struct MutableCacheState {
    pub size_bytes: u64,
    pub files: FileLruCache,
}

#[derive(Clone)]
pub struct CachedStorage<RS>
where
    RS: StorageBackend + Send,
{
    local_storage: LocalStorageBackend,
    remote_storage: RS,
    caches: Vec<CacheState>,
}

pub type CachedAppStorageBackend = CachedStorage<AppStorageBackend>;

impl<RS> CachedStorage<RS>
where
    RS: StorageBackend + Send,
    <RS as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(
        local_storage: LocalStorageBackend,
        remote_storage: RS,
        cache_options: &[CacheOptions],
    ) -> Self {
        let caches = cache_options
            .iter()
            .map(|options| {
                let max_file_count = NonZeroUsize::new(options.max_file_count)
                    .unwrap_or_else(|| NonZeroUsize::new(512).unwrap());

                let mutable = MutableCacheState {
                    size_bytes: 0,
                    files: LruCache::new(max_file_count),
                };
                let mutable = Arc::new(RwLock::new(mutable));

                CacheState {
                    prefix: options.prefix.clone(),
                    max_size_bytes: options.max_size_bytes,
                    mutable,
                }
            })
            .collect();

        Self {
            local_storage,
            remote_storage,
            caches,
        }
    }

    pub async fn cache_size(&mut self, prefix: &str) -> Option<u64> {
        let Some(cache_guard) = self.get_cache(prefix) else {
            return None;
        };

        Some(cache_guard.read_mutable().await.size_bytes)
    }

    pub fn max_cache_size(&mut self, prefix: &str) -> Option<u64> {
        self.get_cache(prefix).map(|c| c.max_size_bytes)
    }

    fn get_cache(&self, prefix: &str) -> Option<&CacheState> {
        self.caches.iter().find(|c| prefix.starts_with(&c.prefix))
    }

    async fn file_promote(&mut self, file_id: &FileID) {
        if let Some(cache) = self.get_cache(&file_id.prefix) {
            cache.promote(&file_id).await;
        }
    }

    async fn file_insert(&mut self, file_id: &FileID, size: u64) -> Result<()> {
        if let Some(cache) = self.get_cache(&file_id.prefix) {
            let files_to_delete = cache.push(&file_id, size).await;
            for file in files_to_delete {
                self.local_storage
                    .remove(file.prefix, file.filename)
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to remove file from cache")?;
            }
        }

        Ok(())
    }

    async fn get_existing_and_promote(
        &mut self,
        prefix: String,
        filename: String,
    ) -> Result<<LocalStorageBackend as StorageBackend>::Reader> {
        let file_id = FileID { prefix, filename };
        self.file_promote(&file_id).await;

        self.local_storage
            .get(file_id.prefix, file_id.filename)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read previously cached file")
    }

    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn download_file_to_cache(&mut self, prefix: &str, filename: &str) -> Result<u64> {
        let mut reader = self.remote_storage.get(&prefix, &filename).await?;
        let mut writer = self.local_storage.put(&prefix, &filename).await?;

        let size = tokio::io::copy(&mut reader, &mut writer)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to copy remote file to local storage")?;
        writer
            .shutdown()
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to shutdown local storage writer")?;

        Ok(size)
    }

    pub async fn mmap(
        &mut self,
        prefix: impl AsRef<str>,
        filename: impl AsRef<str> + Send,
    ) -> Result<Mmap> {
        let prefix = prefix.as_ref().to_string();
        let filename = filename.as_ref().to_string();
        let file_id = FileID {
            prefix: prefix.clone(),
            filename: prefix.clone(),
        };

        if self.local_storage.exists(&prefix, &filename).await? {
            self.file_promote(&file_id).await;
            return self.local_storage.mmap(prefix, filename);
        }

        let size = self.download_file_to_cache(&prefix, &filename).await?;
        self.file_insert(&file_id, size).await?;

        return self.local_storage.mmap(prefix, filename);
    }

    async fn get_new_and_insert(
        &mut self,
        prefix: String,
        filename: String,
        size: u64,
    ) -> Result<<LocalStorageBackend as StorageBackend>::Reader> {
        let file_id = FileID { prefix, filename };
        self.file_insert(&file_id, size).await?;

        self.local_storage
            .get(file_id.prefix, file_id.filename)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read newly cached file")
    }
}

impl CacheState {
    async fn read_mutable(&self) -> RwLockReadGuard<'_, MutableCacheState> {
        self.mutable.read().await
    }

    async fn write_mutable(&self) -> RwLockWriteGuard<'_, MutableCacheState> {
        self.mutable.write().await
    }

    async fn promote(&self, file_id: &FileID) {
        let mut mutable = self.write_mutable().await;
        mutable.files.promote(file_id);
    }

    /// Push the new file entry into the cache.
    ///
    /// Returns a list of files that need to be deleted from the cache.
    async fn push(&self, file_id: &FileID, size: u64) -> Vec<FileID> {
        // Remove files before adding the new one to avoid deleting the new file
        // if it's larger than the cache size.
        let mut to_remove = vec![];

        let mut mutable = self.write_mutable().await;
        while size + mutable.size_bytes >= self.max_size_bytes {
            match mutable.files.pop_lru() {
                None => break,
                Some((file_id, file_size)) => {
                    to_remove.push(file_id);
                    mutable.size_bytes -= file_size;
                }
            }
        }

        mutable.size_bytes += size;

        if let Some((existing_file_id, file_size)) = mutable.files.push(file_id.clone(), size) {
            if &existing_file_id != file_id {
                to_remove.push(existing_file_id);
                mutable.size_bytes -= file_size;
            }
        };

        to_remove
    }
}

#[async_trait]
impl<RS> StorageBackend for CachedStorage<RS>
where
    RS: StorageBackend + Send,
    <RS as StorageBackend>::Reader: Unpin + Send,
{
    type Reader = <LocalStorageBackend as StorageBackend>::Reader;
    type Writer = <LocalStorageBackend as StorageBackend>::Writer;

    async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool> {
        self.remote_storage.exists(prefix, filename).await
    }

    async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Reader> {
        let prefix = prefix.as_ref().to_string();
        let filename = filename.as_ref().to_string();

        if self.local_storage.exists(&prefix, &filename).await? {
            return self.get_existing_and_promote(prefix, filename).await;
        }

        let size = self.download_file_to_cache(&prefix, &filename).await?;

        self.get_new_and_insert(prefix, filename, size).await
    }

    async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Writer> {
        Err(DnaError::Fatal)
            .attach_printable("cached storage is read-only")
            .attach_printable_lazy(|| {
                format!(
                    "hint: attempted to write to {}/{}",
                    prefix.as_ref(),
                    filename.as_ref()
                )
            })
    }
}
