use std::{fs, io::Write, ops::Deref, path::PathBuf, sync::Arc};

use moka::future::Cache;
use rkyv::util::AlignedVec;
use tracing::{debug, warn};
use walkdir::WalkDir;

/// A `mmap` that can be cloned.
#[derive(Clone)]
pub struct Mmap(Arc<memmap2::Mmap>);

#[derive(Debug, Clone)]
pub struct FileCacheOptions {
    pub base_dir: PathBuf,
    pub max_size_bytes: u64,
}

#[derive(Clone)]
pub struct FileCache {
    options: FileCacheOptions,
    cache: Cache<String, Mmap>,
}

impl FileCache {
    pub fn disabled() -> Self {
        let options = FileCacheOptions::default();
        let cache = Cache::<String, Mmap>::builder().max_capacity(0).build();
        Self { options, cache }
    }

    pub fn new(options: FileCacheOptions) -> Self {
        let cache = Cache::<String, Mmap>::builder()
            .max_capacity(options.max_size_bytes)
            .weigher(|_, mmap| mmap.len() as u32)
            .eviction_listener({
                let base_dir = options.base_dir.clone();
                move |relative_path, _, _| {
                    debug!(path = ?relative_path, "evicting file from cache");
                    let _ =
                        fs::remove_file(base_dir.join(relative_path.as_ref())).inspect_err(|err| {
                            warn!(error = ?err, "failed to evict file from cache");
                        });
                }
            })
            .build();

        Self { options, cache }
    }

    pub async fn restore_from_disk(&self) -> std::io::Result<()> {
        for entry in WalkDir::new(&self.options.base_dir)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.is_file() {
                let relative_path = path
                    .strip_prefix(&self.options.base_dir)
                    .map_err(std::io::Error::other)?;
                let key = relative_path
                    .to_str()
                    .ok_or_else(|| std::io::Error::other("failed to convert path to string"))?
                    .to_string();
                let file = fs::File::open(path)?;
                let mmap = Mmap::mmap(&file)?;
                self.cache.insert(key, mmap).await;
            }
        }

        Ok(())
    }

    pub async fn get(&self, path: impl AsRef<str>) -> Option<Mmap> {
        self.cache.get(path.as_ref()).await
    }

    pub async fn contains_key(&self, path: impl AsRef<str>) -> bool {
        self.cache.contains_key(path.as_ref())
    }

    #[tracing::instrument(
        level = "info",
        name = "file_cache_insert",
        skip_all,
        fields(path, data_size, cache_size)
    )]
    pub async fn insert(&self, path: impl Into<String>, data: AlignedVec) -> std::io::Result<Mmap> {
        let current_span = tracing::Span::current();
        let path = path.into();

        current_span.record("path", &path);
        current_span.record("data_size", data.len());
        current_span.record("cache_size", self.weighted_size());

        let file_path = self.options.base_dir.join(&path);
        {
            if let Some(root_dir) = file_path.parent() {
                fs::create_dir_all(root_dir)?;
            }
            let mut file = fs::File::options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&file_path)?;
            file.write_all(&data)?;
            file.flush()?;
        }

        // Reopen the file.
        let file = fs::File::open(&file_path)?;
        let mmap = Mmap::mmap(&file)?;

        self.cache.insert(path, mmap.clone()).await;

        Ok(mmap)
    }

    pub async fn remove(&self, path: impl AsRef<str>) {
        self.cache.remove(path.as_ref()).await;
    }

    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }
}

impl Mmap {
    #[allow(clippy::self_named_constructors)]
    pub fn mmap<T>(file: T) -> Result<Self, std::io::Error>
    where
        T: memmap2::MmapAsRawDesc,
    {
        let inner = unsafe { memmap2::Mmap::map(file) }.map_err(std::io::Error::other)?;
        Ok(Self(Arc::new(inner)))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Mmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl AsRef<[u8]> for Mmap {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl std::fmt::Debug for Mmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mmap")
            .field("len", &self.0.len())
            .field("ptr", &self.0.as_ptr())
            .finish()
    }
}

impl Default for FileCacheOptions {
    fn default() -> Self {
        Self {
            base_dir: dirs::cache_dir()
                .expect("failed to get cache dir")
                .join("dna"),
            max_size_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, DirEntry};

    use rkyv::util::AlignedVec;
    use tempdir::TempDir;

    use super::{FileCache, FileCacheOptions};

    #[tokio::test]
    async fn test_file_cache() {
        let cache_dir = TempDir::new("file_cache_test").unwrap();

        let options = FileCacheOptions {
            base_dir: cache_dir.path().to_path_buf(),
            max_size_bytes: 64,
            ..Default::default()
        };

        let cache = FileCache::new(options);

        for i in 0..12 {
            let mut data = AlignedVec::with_capacity(5);
            data.extend_from_slice(b"hello");
            cache.insert(format!("test-{i}"), data).await.unwrap();
            cache.run_pending_tasks().await;
            assert_eq!(cache.entry_count(), i + 1);
            assert_eq!(cache.weighted_size(), (i + 1) * 5);
        }

        cache.run_pending_tasks().await;

        for i in 0..12 {
            let in_cache = cache.contains_key(format!("test-{i}")).await;
            assert!(in_cache);
        }

        let mut data = AlignedVec::with_capacity(50);
        data.extend_from_slice(b"Chancellor on the brink of second bailout for banks.");
        cache.insert(format!("large"), data).await.unwrap();

        // Make large the most used file.
        for _ in 0..5 {
            cache.get(format!("large")).await.unwrap();
        }

        cache.run_pending_tasks().await;
        assert_eq!(cache.weighted_size(), 57);

        for i in 0..11 {
            assert!(!cache.contains_key(format!("test-{i}")).await);
        }

        assert!(cache.contains_key("test-11").await);

        let files = fs::read_dir(cache_dir.path())
            .unwrap()
            .collect::<Vec<std::io::Result<DirEntry>>>();
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_file_cache_restore_from_disk() {
        let cache_dir = TempDir::new("file_cache_test").unwrap();

        let options = FileCacheOptions {
            base_dir: cache_dir.path().to_path_buf(),
            max_size_bytes: 64,
            ..Default::default()
        };

        {
            let cache = FileCache::new(options.clone());

            for i in 0..12 {
                let mut data = AlignedVec::with_capacity(5);
                data.extend_from_slice(b"hello");
                cache
                    .insert(format!("test/folder/test-{i}"), data)
                    .await
                    .unwrap();
            }

            cache.run_pending_tasks().await;
            assert_eq!(cache.weighted_size(), 60);
            assert_eq!(cache.entry_count(), 12);
        }

        let cache = FileCache::new(options);
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.entry_count(), 0);

        cache.restore_from_disk().await.unwrap();
        cache.run_pending_tasks().await;

        assert_eq!(cache.weighted_size(), 60);
        assert_eq!(cache.entry_count(), 12);

        assert!(cache.contains_key("test/folder/test-0").await);
    }
}