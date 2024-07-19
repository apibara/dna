use std::path::PathBuf;

use crate::{
    error::{DnaError, Result},
    storage::{
        AppStorageBackend, AzureStorageBackendBuilder, LocalStorageBackend, S3StorageBackendBuilder,
    },
};
use byte_unit::Byte;
use clap::Args;
use error_stack::ResultExt;

use super::CacheOptions;

#[derive(Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct StorageArgs {
    #[arg(long, env)]
    local_dir: Option<PathBuf>,
    #[arg(long, env)]
    azure_container: Option<String>,
    #[arg(long, env)]
    s3_bucket: Option<String>,
}

impl StorageArgs {
    pub fn to_app_storage_backend(&self) -> Result<AppStorageBackend> {
        if let Some(local_dir) = &self.local_dir {
            let storage = LocalStorageBackend::new(local_dir);
            Ok(AppStorageBackend::Local(storage))
        } else if let Some(azure_container) = &self.azure_container {
            let storage = AzureStorageBackendBuilder::from_env()
                .with_container_name(azure_container)
                .build()
                .change_context(DnaError::Fatal)
                .attach_printable("failed to build Azure storage backend")
                .attach_printable("hint: have you set the AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables?")?;
            Ok(AppStorageBackend::Azure(storage))
        } else if let Some(s3_bucket) = &self.s3_bucket {
            let storage = S3StorageBackendBuilder::from_env()
                .with_bucket_name(s3_bucket)
                .build()
                .change_context(DnaError::Fatal)
                .attach_printable("failed to build S3 storage backend")
                .attach_printable("hint: have you set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables?")?;
            Ok(AppStorageBackend::S3(storage))
        } else {
            Err(DnaError::Configuration)
                .attach_printable("no storage backend configured")
                .change_context(DnaError::Configuration)
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct CacheArgs {
    /// Location for cached/temporary data.
    #[arg(long, env)]
    cache_dir: PathBuf,
    /// Maximum number of group files in the cache.
    #[arg(long, env)]
    cache_max_group_files: Option<usize>,
    /// Maximum storage size of group files in the cache.
    #[arg(long, env)]
    cache_max_group_size: Option<String>,
    /// Maximum number of segment files in the cache.
    #[arg(long, env)]
    cache_max_segment_files: Option<usize>,
    /// Maximum storage size of segment files in the cache.
    #[arg(long, env)]
    cache_max_segment_size: Option<String>,
}

impl CacheArgs {
    pub fn to_local_storage_backend(&self) -> LocalStorageBackend {
        LocalStorageBackend::new(&self.cache_dir)
    }

    pub fn to_cache_options(&self) -> Result<Vec<CacheOptions>> {
        let max_group_files = self.cache_max_group_files.unwrap_or(1_000);
        let max_group_size = if let Some(max_group_size) = self.cache_max_group_size.as_ref() {
            Byte::from_str(max_group_size)
                .change_context(DnaError::Configuration)
                .attach_printable("failed to parse cache max group size")?
        } else {
            Byte::from_bytes(1_000_000_000)
        };

        let group_options = CacheOptions {
            prefix: "group".to_string(),
            max_file_count: max_group_files,
            max_size_bytes: max_group_size.get_bytes() as u64,
        };

        let max_segment_files = self.cache_max_segment_files.unwrap_or(1_000);
        let max_segment_size = if let Some(max_segment_size) = self.cache_max_segment_size.as_ref()
        {
            Byte::from_str(max_segment_size)
                .change_context(DnaError::Configuration)
                .attach_printable("failed to parse cache max segment size")?
        } else {
            Byte::from_bytes(1_000_000_000)
        };

        let segment_options = CacheOptions {
            prefix: "segment".to_string(),
            max_file_count: max_segment_files,
            max_size_bytes: max_segment_size.get_bytes() as u64,
        };

        Ok(vec![group_options, segment_options])
    }
}
