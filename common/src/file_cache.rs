use std::{path::PathBuf, str::FromStr, sync::Arc};

use apibara_observability::mixtrics_registry;
use bytes::Bytes;
use clap::Args;
use error_stack::{Result, ResultExt};
use foyer::{
    AdmissionPicker, AdmitAllPicker, CacheEntry, Compression, DirectFsDeviceOptions, Engine,
    HybridCache, HybridCacheBuilder, HybridFetch, LargeEngineOptions, RateLimitPicker, RecoverMode,
    RuntimeOptions, S3FifoConfig, TokioRuntimeOptions,
};

#[derive(Debug)]
pub enum FileCacheError {
    Config,
    Foyer(anyhow::Error),
}

/// A cache with the content of remote files.
#[derive(Clone)]
pub struct FileCache {
    /// Cache used for group inedx files.
    pub index: HybridCache<String, Bytes>,
    /// Cache used fora all other files.
    pub general: HybridCache<String, Bytes>,
}

pub type FileFetch = HybridFetch<String, Bytes>;

pub type CachedFile = CacheEntry<String, Bytes>;

#[derive(Args, Debug)]
pub struct FileCacheArgs {
    /// Where to store cached data.
    #[clap(long = "cache.dir", env = "DNA_CACHE_DIR")]
    pub cache_dir: Option<String>,
    /// Maximum size for the on-disk data cache.
    #[clap(
        long = "cache.data-disk-size",
        env = "DNA_CACHE_DATA_DISK_SIZE",
        default_value = "10Gi"
    )]
    pub cache_data_disk_size: String,
    /// Size of the direct fs files (data cache).
    #[clap(
        long = "cache.data-file-size",
        env = "DNA_CACHE_DATA_FILE_SIZE",
        default_value = "1Gi"
    )]
    pub cache_data_file_size: String,
    /// Size of the in-memory data cache.
    #[clap(
        long = "cache.data-memory-size",
        env = "DNA_CACHE_DATA_MEMORY_SIZE",
        default_value = "2Gi"
    )]
    pub cache_data_memory_size: String,
    /// Maximum size for the on-disk index cache.
    #[clap(
        long = "cache.index-disk-size",
        env = "DNA_CACHE_INDEX_DISK_SIZE",
        default_value = "10Gi"
    )]
    pub cache_index_disk_size: String,
    /// Size of the direct fs files (index cache).
    #[clap(
        long = "cache.index-file-size",
        env = "DNA_CACHE_INDEX_FILE_SIZE",
        default_value = "1Gi"
    )]
    pub cache_index_file_size: String,
    /// Size of the in-memory index cache.
    #[clap(
        long = "cache.index-memory-size",
        env = "DNA_CACHE_INDEX_MEMORY_SIZE",
        default_value = "2Gi"
    )]
    pub cache_index_memory_size: String,
    /// Cache worker threads for reading.
    ///
    /// Data and index cache use the same number of threads.
    #[clap(
        long = "cache.runtime-read-threads",
        env = "DNA_CACHE_RUNTIME_READ_THREADS",
        default_value = "4"
    )]
    pub cache_runtime_read_threads: usize,
    /// Cache worker threads for writing.
    ///
    /// Data and index cache use the same number of threads.
    #[clap(
        long = "cache.runtime-write-threads",
        env = "DNA_CACHE_RUNTIME_WRITE_THREADS",
        default_value = "4"
    )]
    pub cache_runtime_write_threads: usize,
    /// Set how fast items can be inserted into the cache.
    ///
    /// This value is in bytes per second, e.g. `100Mi` to insert data at 100 MiB per second.
    #[clap(
        long = "cache.admission-rate-limit",
        env = "DNA_CACHE_ADMISSION_RATE_LIMIT"
    )]
    pub cache_admission_rate_limit: Option<String>,
    /// Set the compression algorithm.
    ///
    /// One of: none, lz4, zstd.
    #[clap(
        long = "cache.compression",
        env = "DNA_CACHE_COMPRESSION",
        default_value = "zstd"
    )]
    pub cache_compression: String,
    /// Enable `sync` after writes.
    #[clap(long = "cache.flush", env = "DNA_CACHE_FLUSH")]
    pub cache_flush: bool,
    /// Set the flusher count.
    #[clap(
        long = "cache.flusher-count",
        env = "DNA_CACHE_FLUSHER_COUNT",
        default_value = "2"
    )]
    pub cache_flusher_count: usize,
    /// Set the flush buffer pool size.
    #[clap(
        long = "cache.flush-buffer-pool-size",
        env = "DNA_CACHE_FLUSH_BUFFER_POOL_SIZE",
        default_value = "1Gi"
    )]
    pub cache_flush_buffer_pool_size: String,
}

impl FileCacheArgs {
    pub async fn to_file_cache(&self) -> Result<FileCache, FileCacheError> {
        let cache_dir = if let Some(cache_dir) = &self.cache_dir {
            cache_dir
                .parse::<PathBuf>()
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse cache dir")
                .attach_printable_lazy(|| format!("cache dir: {}", cache_dir))?
        } else {
            dirs::data_local_dir()
                .ok_or(FileCacheError::Config)
                .attach_printable("failed to get data dir")?
                .join("dna")
        };

        let compression = match self.cache_compression.as_str() {
            "none" => Compression::None,
            "lz4" => Compression::Lz4,
            "zstd" => Compression::Zstd,
            _ => Err(FileCacheError::Config)
                .attach_printable("failed to parse compression")
                .attach_printable_lazy(|| format!("compression: {}", self.cache_compression))?,
        };

        let flush_buffer_pool_size = byte_unit::Byte::from_str(&self.cache_flush_buffer_pool_size)
            .change_context(FileCacheError::Config)
            .attach_printable("failed to parse flush buffer pool size")
            .attach_printable_lazy(|| {
                format!(
                    "flush buffer pool size: {}",
                    self.cache_flush_buffer_pool_size
                )
            })?
            .as_u64();

        let general = {
            let admission_picker: Arc<dyn AdmissionPicker<Key = String>> =
                if let Some(rate_limit) = self.cache_admission_rate_limit.as_ref() {
                    let rate_limit = byte_unit::Byte::from_str(rate_limit)
                        .change_context(FileCacheError::Config)
                        .attach_printable("failed to parse admission rate limit")
                        .attach_printable_lazy(|| format!("rate limit: {}", rate_limit))?
                        .as_u64();
                    Arc::new(RateLimitPicker::new(rate_limit as usize))
                } else {
                    Arc::new(AdmitAllPicker::default())
                };

            let max_size_memory_bytes = byte_unit::Byte::from_str(&self.cache_data_memory_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse in memory data cache size")
                .attach_printable_lazy(|| {
                    format!("data cache size: {}", self.cache_data_memory_size)
                })?
                .as_u64();

            let max_size_disk_bytes = byte_unit::Byte::from_str(&self.cache_data_disk_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse on disk data cache size")
                .attach_printable_lazy(|| {
                    format!("data cache size: {}", self.cache_data_disk_size)
                })?
                .as_u64();

            let file_size = byte_unit::Byte::from_str(&self.cache_data_file_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse data cache file size")
                .attach_printable_lazy(|| format!("file size: {}", self.cache_data_file_size))?
                .as_u64();

            HybridCacheBuilder::new()
                .with_name("general")
                .with_metrics_registry(mixtrics_registry("file_cache"))
                .memory(max_size_memory_bytes as usize)
                .with_eviction_config(S3FifoConfig::default())
                .with_weighter(|_: &String, bytes: &Bytes| bytes.len())
                .storage(Engine::Large)
                .with_compression(compression)
                .with_admission_picker(admission_picker)
                .with_runtime_options(RuntimeOptions::Separated {
                    read_runtime_options: TokioRuntimeOptions {
                        worker_threads: self.cache_runtime_read_threads,
                        max_blocking_threads: self.cache_runtime_read_threads * 2,
                    },
                    write_runtime_options: TokioRuntimeOptions {
                        worker_threads: self.cache_runtime_write_threads,
                        max_blocking_threads: self.cache_runtime_write_threads * 2,
                    },
                })
                .with_large_object_disk_cache_options(
                    LargeEngineOptions::new()
                        .with_flushers(self.cache_flusher_count)
                        .with_buffer_pool_size(flush_buffer_pool_size as usize),
                )
                .with_flush(self.cache_flush)
                .with_device_options(
                    DirectFsDeviceOptions::new(cache_dir.join("general"))
                        .with_capacity(max_size_disk_bytes as usize)
                        .with_file_size(file_size as usize),
                )
                .with_recover_mode(RecoverMode::Quiet)
                .build()
                .await
                .map_err(FileCacheError::Foyer)
        }?;

        let index = {
            let admission_picker: Arc<dyn AdmissionPicker<Key = String>> =
                if let Some(rate_limit) = self.cache_admission_rate_limit.as_ref() {
                    let rate_limit = byte_unit::Byte::from_str(rate_limit)
                        .change_context(FileCacheError::Config)
                        .attach_printable("failed to parse admission rate limit")
                        .attach_printable_lazy(|| format!("rate limit: {}", rate_limit))?
                        .as_u64();
                    Arc::new(RateLimitPicker::new(rate_limit as usize))
                } else {
                    Arc::new(AdmitAllPicker::default())
                };

            let max_size_memory_bytes = byte_unit::Byte::from_str(&self.cache_index_memory_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse in memory index cache size")
                .attach_printable_lazy(|| {
                    format!("index cache size: {}", self.cache_index_memory_size)
                })?
                .as_u64();

            let max_size_disk_bytes = byte_unit::Byte::from_str(&self.cache_index_disk_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse on disk index cache size")
                .attach_printable_lazy(|| {
                    format!("index cache size: {}", self.cache_index_disk_size)
                })?
                .as_u64();

            let file_size = byte_unit::Byte::from_str(&self.cache_index_file_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse index cache file size")
                .attach_printable_lazy(|| {
                    format!("index file size: {}", self.cache_index_file_size)
                })?
                .as_u64();

            HybridCacheBuilder::new()
                .with_name("index")
                .with_metrics_registry(mixtrics_registry("file_cache"))
                .memory(max_size_memory_bytes as usize)
                .with_eviction_config(S3FifoConfig::default())
                .with_weighter(|_: &String, bytes: &Bytes| bytes.len())
                .storage(Engine::Large)
                .with_compression(compression)
                .with_admission_picker(admission_picker)
                .with_runtime_options(RuntimeOptions::Separated {
                    read_runtime_options: TokioRuntimeOptions {
                        worker_threads: self.cache_runtime_read_threads,
                        max_blocking_threads: self.cache_runtime_read_threads * 2,
                    },
                    write_runtime_options: TokioRuntimeOptions {
                        worker_threads: self.cache_runtime_write_threads,
                        max_blocking_threads: self.cache_runtime_write_threads * 2,
                    },
                })
                .with_large_object_disk_cache_options(
                    LargeEngineOptions::new()
                        .with_flushers(self.cache_flusher_count)
                        .with_buffer_pool_size(flush_buffer_pool_size as usize),
                )
                .with_flush(self.cache_flush)
                .with_device_options(
                    DirectFsDeviceOptions::new(cache_dir.join("index"))
                        .with_capacity(max_size_disk_bytes as usize)
                        .with_file_size(file_size as usize),
                )
                .with_recover_mode(RecoverMode::Quiet)
                .build()
                .await
                .map_err(FileCacheError::Foyer)
        }?;

        Ok(FileCache { general, index })
    }
}

impl error_stack::Context for FileCacheError {}

impl std::fmt::Display for FileCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileCacheError::Config => write!(f, "file cache builder error: config error"),
            FileCacheError::Foyer(err) => write!(f, "file cache builder error: {}", err),
        }
    }
}
