use std::num::NonZeroUsize;

use error_stack::ResultExt;
use lru::LruCache;
use tonic::async_trait;

use crate::error::{DnaError, Result};

use super::{LocalStorageBackend, StorageBackend};

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

struct CacheState {
    pub prefix: String,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
    pub files: LruCache<FileID, u64>,
}

pub struct CachedStorage<RS>
where
    RS: StorageBackend + Send,
{
    local_storage: LocalStorageBackend,
    remote_storage: RS,
    caches: Vec<CacheState>,
}

impl<RS> CachedStorage<RS>
where
    RS: StorageBackend + Send,
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
                    size_bytes: 0,
                    max_size_bytes: options.max_size_bytes,
                    files: LruCache::new(max_file_count),
                }
            })
            .collect();

        Self {
            local_storage,
            remote_storage,
            caches,
        }
    }

    pub fn cache_size(&mut self, prefix: &str) -> Option<u64> {
        self.get_cache(prefix).map(|c| c.size_bytes)
    }

    pub fn max_cache_size(&mut self, prefix: &str) -> Option<u64> {
        self.get_cache(prefix).map(|c| c.max_size_bytes)
    }

    fn get_cache(&mut self, prefix: &str) -> Option<&mut CacheState> {
        self.caches
            .iter_mut()
            .find(|c| prefix.starts_with(&c.prefix))
    }

    async fn get_existing_and_promote(
        &mut self,
        prefix: String,
        filename: String,
    ) -> Result<<LocalStorageBackend as StorageBackend>::Reader> {
        let file_id = FileID { prefix, filename };

        if let Some(cache) = self.get_cache(&file_id.prefix) {
            cache.promote(&file_id);
        }

        self.local_storage
            .get(file_id.prefix, file_id.filename)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read previously cached file")
    }

    async fn get_new_and_insert(
        &mut self,
        prefix: String,
        filename: String,
        size: u64,
    ) -> Result<<LocalStorageBackend as StorageBackend>::Reader> {
        let file_id = FileID { prefix, filename };

        if let Some(cache) = self.get_cache(&file_id.prefix) {
            let files_to_delete = cache.push(&file_id, size);
            for file in files_to_delete {
                self.local_storage
                    .remove(file.prefix, file.filename)
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to remove file from cache")?;
            }
        }

        self.local_storage
            .get(file_id.prefix, file_id.filename)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read newly cached file")
    }
}

impl CacheState {
    fn promote(&mut self, file_id: &FileID) {
        self.files.promote(file_id);
    }

    /// Push the new file entry into the cache.
    ///
    /// Returns a list of files that need to be deleted from the cache.
    fn push(&mut self, file_id: &FileID, size: u64) -> Vec<FileID> {
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

        let mut reader = self.remote_storage.get(&prefix, &filename).await?;
        let mut writer = self.local_storage.put(&prefix, &filename).await?;

        let size = tokio::io::copy(&mut reader, &mut writer)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to copy remote file to local storage")?;

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
