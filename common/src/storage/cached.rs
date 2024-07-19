use std::{num::NonZeroUsize, sync::Arc};

use error_stack::ResultExt;
use lru::LruCache;
use memmap2::Mmap;
use tokio::{io::AsyncWriteExt, sync::Mutex};
use tracing::{debug, level_filters::LevelFilter};

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
    pub size_bytes: u64,
    pub files: FileLruCache,
}

#[derive(Clone)]
struct InnerStorage<RS>
where
    RS: StorageBackend + Send,
{
    local_storage: LocalStorageBackend,
    remote_storage: RS,
    caches: Vec<CacheState>,
}

/// Cached storage.
///
/// Notice that the current implementation locks all operations on the cache.
/// This limits the performance since operations on different files will have
/// to wait for the lock to be released.
/// On the other hand, this is needed to avoid multiple tasks writing to the
/// same file at the same time and corrupting it.
#[derive(Clone)]
pub struct CachedStorage<RS>(Arc<Mutex<InnerStorage<RS>>>)
where
    RS: StorageBackend + Send;

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

                CacheState {
                    prefix: options.prefix.clone(),
                    max_size_bytes: options.max_size_bytes,
                    size_bytes: 0,
                    files: LruCache::new(max_file_count),
                }
            })
            .collect();

        let inner = InnerStorage {
            local_storage,
            remote_storage,
            caches,
        };

        Self(Arc::new(Mutex::new(inner)))
    }

    pub async fn mmap(
        &mut self,
        prefix: impl AsRef<str>,
        filename: impl AsRef<str> + Send,
    ) -> Result<Mmap> {
        let prefix = prefix.as_ref().to_string();
        let filename = filename.as_ref().to_string();
        let file_id = FileID { prefix, filename };

        let mut inner = self.0.lock().await;

        if inner.exists_locally(&file_id).await? {
            debug!(prefix = %file_id.prefix, filename = %file_id.filename, "file exists locally");
            inner.file_promote(&file_id).await;
            return inner.mmap_local(&file_id);
        }

        debug!(prefix = %file_id.prefix, filename = %file_id.filename, "adding new file to cache");
        let size = inner.download_file_to_cache(&file_id).await?;
        inner.file_insert(&file_id, size).await?;

        inner.mmap_local(&file_id)
    }
}

impl<RS> InnerStorage<RS>
where
    RS: StorageBackend + Send,
    <RS as StorageBackend>::Reader: Unpin + Send,
{
    async fn exists_locally(&mut self, file_id: &FileID) -> Result<bool> {
        self.local_storage
            .exists(&file_id.prefix, &file_id.filename)
            .await
    }

    fn mmap_local(&self, file_id: &FileID) -> Result<Mmap> {
        self.local_storage.mmap(&file_id.prefix, &file_id.filename)
    }

    fn get_cache_mut(&mut self, prefix: &str) -> Option<&mut CacheState> {
        self.caches
            .iter_mut()
            .find(|c| prefix.starts_with(&c.prefix))
    }

    fn get_cache(&self, prefix: &str) -> Option<&CacheState> {
        self.caches.iter().find(|c| prefix.starts_with(&c.prefix))
    }

    pub fn cache_size(&mut self, prefix: &str) -> Option<u64> {
        let Some(cache_guard) = self.get_cache(prefix) else {
            return None;
        };

        Some(cache_guard.size_bytes)
    }

    pub fn max_cache_size(&mut self, prefix: &str) -> Option<u64> {
        self.get_cache(prefix).map(|c| c.max_size_bytes)
    }

    async fn file_promote(&mut self, file_id: &FileID) {
        if let Some(cache) = self.get_cache_mut(&file_id.prefix) {
            cache.promote(file_id).await;
        }
    }

    async fn file_insert(&mut self, file_id: &FileID, size: u64) -> Result<()> {
        if LevelFilter::current() >= LevelFilter::DEBUG {
            let cache_size = self.cache_size(&file_id.prefix);
            let max_cache_size = self.max_cache_size(&file_id.prefix);
            debug!(
                prefix = %file_id.prefix,
                filename = %file_id.filename,
                cache_size = ?cache_size,
                max_cache_size = ?max_cache_size,
                "inserting file into cache"
            );
        }

        if let Some(cache) = self.get_cache_mut(&file_id.prefix) {
            let files_to_delete = cache.push(file_id, size).await;
            for file in files_to_delete {
                debug!(prefix = %file.prefix, filename = %file.filename, "deleting file from cache");
                self.local_storage
                    .remove(file.prefix, file.filename)
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to remove file from cache")?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn download_file_to_cache(&mut self, file_id: &FileID) -> Result<u64> {
        let mut reader = self
            .remote_storage
            .get(&file_id.prefix, &file_id.filename)
            .await?;
        let mut writer = self
            .local_storage
            .put(&file_id.prefix, &file_id.filename)
            .await?;

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
}

impl CacheState {
    async fn promote(&mut self, file_id: &FileID) {
        self.files.promote(file_id);
    }

    /// Push the new file entry into the cache.
    ///
    /// Returns a list of files that need to be deleted from the cache.
    async fn push(&mut self, file_id: &FileID, size: u64) -> Vec<FileID> {
        // Remove files before adding the new one to avoid deleting the new file
        // if it's larger than the cache size.
        let mut to_remove = vec![];

        while size + self.size_bytes >= self.max_size_bytes {
            match self.files.pop_lru() {
                None => break,
                Some((file_id, file_size)) => {
                    to_remove.push(file_id);
                    self.size_bytes -= file_size;
                }
            }
        }

        self.size_bytes += size;

        if let Some((existing_file_id, file_size)) = self.files.push(file_id.clone(), size) {
            if &existing_file_id != file_id {
                to_remove.push(existing_file_id);
                self.size_bytes -= file_size;
            }
        };

        to_remove
    }
}

/*
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

*/
