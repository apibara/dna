use tokio::io::{AsyncRead, AsyncWrite};
use tonic::async_trait;

use super::{AzureStorageBackend, LocalStorageBackend, S3StorageBackend, StorageBackend};
use crate::error::Result;

/// A storage backend that can be used by the application.
#[derive(Clone)]
pub enum AppStorageBackend {
    Local(LocalStorageBackend),
    S3(S3StorageBackend),
    Azure(AzureStorageBackend),
}

#[async_trait]
impl StorageBackend for AppStorageBackend {
    type Reader = Box<dyn AsyncRead + Unpin + Send>;
    type Writer = Box<dyn AsyncWrite + Unpin + Send>;

    async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool> {
        match self {
            Self::Local(storage) => storage.exists(prefix, filename).await,
            Self::S3(storage) => storage.exists(prefix, filename).await,
            Self::Azure(storage) => storage.exists(prefix, filename).await,
        }
    }

    async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Reader> {
        match self {
            Self::Local(storage) => {
                let reader = storage.get(prefix, filename).await?;
                Ok(Box::new(reader))
            }
            Self::S3(storage) => storage.get(prefix, filename).await,
            Self::Azure(storage) => storage.get(prefix, filename).await,
        }
    }

    async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Writer> {
        match self {
            Self::Local(storage) => {
                let writer = storage.put(prefix, filename).await?;
                Ok(Box::new(writer))
            }
            Self::S3(storage) => storage.put(prefix, filename).await,
            Self::Azure(storage) => storage.put(prefix, filename).await,
        }
    }
}
