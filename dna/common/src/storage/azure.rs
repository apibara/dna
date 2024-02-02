use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use async_trait::async_trait;
use error_stack::ResultExt;
use object_store::{
    azure::{MicrosoftAzure, MicrosoftAzureBuilder},
    path::Path,
    ObjectStore,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::StreamReader;

use crate::error::{DnaError, Result};

use super::StorageBackend;

#[derive(Default)]
pub struct AzureStorageBackendBuilder(MicrosoftAzureBuilder);

impl AzureStorageBackendBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_env() -> Self {
        Self(MicrosoftAzureBuilder::from_env())
    }

    pub fn with_container_name(mut self, container_name: impl Into<String>) -> Self {
        self.0 = self.0.with_container_name(container_name);
        self
    }

    pub fn with_account(mut self, account: impl Into<String>) -> Self {
        self.0 = self.0.with_account(account);
        self
    }

    pub fn with_access_key(mut self, access_key: impl Into<String>) -> Self {
        self.0 = self.0.with_access_key(access_key);
        self
    }

    pub fn with_allow_http(mut self, allow: bool) -> Self {
        self.0 = self.0.with_allow_http(allow);
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.0 = self.0.with_endpoint(endpoint.into());
        self
    }

    pub fn build(self) -> Result<AzureStorageBackend> {
        let inner = self
            .0
            .build()
            .change_context(DnaError::Configuration)
            .attach_printable("failed to build Azure storage backend")?;
        Ok(AzureStorageBackend { inner })
    }
}

pub struct AzureStorageBackend {
    inner: MicrosoftAzure,
}

impl AzureStorageBackend {
    pub async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool> {
        let path = Path::from(format!("{}/{}", prefix.as_ref(), filename.as_ref()));
        match self.inner.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e)
                .change_context(DnaError::Io)
                .attach_printable_lazy(|| format!("failed to check if file exists: {:?}", path)),
        }
    }

    pub async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        let path = Path::from(format!("{}/{}", prefix.as_ref(), filename.as_ref()));
        let response = self
            .inner
            .get(&path)
            .await
            .change_context(DnaError::Io)
            .attach_printable_lazy(|| format!("failed to get file: {:?}", path))?;
        let stream = response.into_stream();
        let reader = StreamReader::new(stream);

        let decompressed = ZstdDecoder::new(reader);
        Ok(Box::new(decompressed))
    }

    pub async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Box<dyn AsyncWrite + Unpin + Send>> {
        let path = Path::from(format!("{}/{}", prefix.as_ref(), filename.as_ref()));
        let (_, writer) = self
            .inner
            .put_multipart(&path)
            .await
            .change_context(DnaError::Io)
            .attach_printable_lazy(|| format!("failed to put file: {:?}", path))?;

        let compressed = ZstdEncoder::new(writer);

        Ok(Box::new(compressed))
    }
}

#[async_trait]
impl StorageBackend for AzureStorageBackend {
    type Reader = Box<dyn AsyncRead + Unpin + Send>;
    type Writer = Box<dyn AsyncWrite + Unpin + Send>;

    async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool> {
        self.exists(prefix, filename).await
    }

    async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Reader> {
        self.get(prefix, filename).await
    }

    async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Writer> {
        self.put(prefix, filename).await
    }
}
