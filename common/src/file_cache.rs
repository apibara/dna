use std::{path::PathBuf, str::FromStr};

use apibara_observability::mixtrics_registry;
use bytes::Bytes;
use clap::Args;
use error_stack::{Result, ResultExt};
use foyer::{
    BlockEngineConfig, CacheEntry, Compression, DeviceBuilder, FsDeviceBuilder, HybridCache,
    HybridCacheBuilder, HybridGetOrFetch, PsyncIoEngineConfig, RecoverMode, S3FifoConfig,
};

#[derive(Debug)]
pub enum FileCacheError {
    Config,
    Foyer(foyer::Error),
}

/// A cache with the content of remote files.
#[derive(Clone)]
pub struct FileCache {
    /// Cache used for group index files.
    pub index: HybridCache<String, Bytes>,
    /// Cache used fora all other files.
    pub general: HybridCache<String, Bytes>,
}

pub type FileFetch = HybridGetOrFetch<String, Bytes>;

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
    /// Size of the direct fs blocks (data cache).
    #[clap(
        long = "cache.data-block-size",
        env = "DNA_CACHE_DATA_BLOCK_SIZE",
        default_value = "128Mi"
    )]
    pub cache_data_block_size: String,
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
    /// Size of the direct fs blocks (index cache).
    #[clap(
        long = "cache.index-block-size",
        env = "DNA_CACHE_INDEX_BLOCK_SIZE",
        default_value = "128Mi"
    )]
    pub cache_index_block_size: String,
    /// Size of the in-memory index cache.
    #[clap(
        long = "cache.index-memory-size",
        env = "DNA_CACHE_INDEX_MEMORY_SIZE",
        default_value = "2Gi"
    )]
    pub cache_index_memory_size: String,
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

            let block_size = byte_unit::Byte::from_str(&self.cache_data_block_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse on disk data block size")
                .attach_printable_lazy(|| {
                    format!("data cache block size: {}", self.cache_data_block_size)
                })?
                .as_u64();

            let device = FsDeviceBuilder::new(cache_dir.join("general"))
                .with_capacity(max_size_disk_bytes as _)
                .build()
                .map_err(FileCacheError::Foyer)?;

            HybridCacheBuilder::<String, Bytes>::new()
                .with_name("general")
                .with_metrics_registry(mixtrics_registry("file_cache"))
                .memory(max_size_memory_bytes as _)
                .with_eviction_config(S3FifoConfig::default())
                .with_weighter(|_: &String, bytes: &Bytes| bytes.len())
                .storage()
                .with_compression(compression)
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(
                    BlockEngineConfig::new(device)
                        .with_block_size(block_size as _)
                        .with_flushers(self.cache_flusher_count)
                        .with_buffer_pool_size(flush_buffer_pool_size as _),
                )
                .with_recover_mode(RecoverMode::Quiet)
                .build()
                .await
                .map_err(FileCacheError::Foyer)
        }?;

        let index = {
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

            let block_size = byte_unit::Byte::from_str(&self.cache_index_block_size)
                .change_context(FileCacheError::Config)
                .attach_printable("failed to parse on disk index block size")
                .attach_printable_lazy(|| {
                    format!("data cache block size: {}", self.cache_index_block_size)
                })?
                .as_u64();

            let device = FsDeviceBuilder::new(cache_dir.join("general"))
                .with_capacity(max_size_disk_bytes as usize)
                .build()
                .map_err(FileCacheError::Foyer)?;

            HybridCacheBuilder::<String, Bytes>::new()
                .with_name("index")
                .with_metrics_registry(mixtrics_registry("file_cache"))
                .memory(max_size_memory_bytes as usize)
                .with_eviction_config(S3FifoConfig::default())
                .with_weighter(|_: &String, bytes: &Bytes| bytes.len())
                .storage()
                .with_compression(compression)
                .with_io_engine_config(PsyncIoEngineConfig::new())
                .with_engine_config(
                    BlockEngineConfig::new(device)
                        .with_block_size(block_size as _)
                        .with_flushers(self.cache_flusher_count)
                        .with_buffer_pool_size(flush_buffer_pool_size as _),
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
