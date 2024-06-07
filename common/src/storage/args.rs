use std::path::PathBuf;

use crate::{
    error::{DnaError, Result},
    storage::{
        AppStorageBackend, AzureStorageBackendBuilder, LocalStorageBackend, S3StorageBackendBuilder,
    },
};
use clap::Args;
use error_stack::ResultExt;

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
    #[arg(long, env)]
    cache_dir: PathBuf,
}

impl CacheArgs {
    pub fn to_local_storage_backend(&self) -> LocalStorageBackend {
        LocalStorageBackend::new(&self.cache_dir)
    }
}
