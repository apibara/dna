use bytes::Bytes;
use error_stack::Result;

use super::{
    azure_blob::AzureBlobClient, AwsS3Client, DeleteOptions, GetOptions, ObjectETag,
    ObjectStoreError, PutOptions,
};

#[derive(Clone)]
pub enum ObjectStoreClient {
    AwsS3(AwsS3Client),
    AzureBlob(AzureBlobClient),
}

impl ObjectStoreClient {
    pub async fn create_bucket(&self, name: &str) -> Result<(), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.create_bucket(name).await,
            Self::AzureBlob(client) => client.create_bucket(name).await,
        }
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        options: GetOptions,
    ) -> Result<(ObjectETag, Bytes), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.get_object(bucket, key, options).await,
            Self::AzureBlob(client) => client.get_object(bucket, key, options).await,
        }
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<ObjectETag, ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.put_object(bucket, key, body, options).await,
            Self::AzureBlob(client) => client.put_object(bucket, key, body, options).await,
        }
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        _options: DeleteOptions,
    ) -> Result<(), ObjectStoreError> {
        match self {
            Self::AwsS3(client) => client.delete_object(bucket, key, _options).await,
            Self::AzureBlob(client) => client.delete_object(bucket, key, _options).await,
        }
    }
}

impl From<AwsS3Client> for ObjectStoreClient {
    fn from(client: AwsS3Client) -> Self {
        Self::AwsS3(client)
    }
}

impl From<AzureBlobClient> for ObjectStoreClient {
    fn from(client: AzureBlobClient) -> Self {
        Self::AzureBlob(client)
    }
}
