use azure_core::{self, prelude::IfMatchCondition};
use azure_storage::{CloudLocation, StorageCredentials};
use azure_storage_blobs::prelude::ClientBuilder;
use bytes::{Bytes, BytesMut};
use error_stack::{Result, ResultExt};
use futures::TryStreamExt;
use tracing::debug;

use crate::object_store::ObjectStoreResultExt;

use super::{
    error::ToObjectStoreResult, metrics::ObjectStoreMetrics, DeleteOptions, GetOptions, ObjectETag,
    ObjectStoreError, PutMode, PutOptions,
};

#[derive(Clone)]
pub struct AzureBlobClient {
    client: ClientBuilder,
    metrics: ObjectStoreMetrics,
}

impl AzureBlobClient {
    pub fn new(
        location: impl Into<CloudLocation>,
        credentials: impl Into<StorageCredentials>,
    ) -> Self {
        let client = ClientBuilder::with_location(location.into(), credentials.into());
        AzureBlobClient {
            client,
            metrics: ObjectStoreMetrics::default(),
        }
    }

    pub async fn has_bucket(&self, name: &str) -> Result<bool, ObjectStoreError> {
        let properties = match self
            .client
            .clone()
            .container_client(name)
            .get_properties()
            .await
            .change_to_object_store_context()
        {
            Ok(properties) => properties,
            Err(err) if err.is_not_found() => return Ok(false),
            Err(err) => return Err(err),
        };

        debug!(properties = ?properties, "azure blob storage container exists");

        Ok(true)
    }

    pub async fn create_bucket(&self, name: &str) -> Result<(), ObjectStoreError> {
        self.client
            .clone()
            .container_client(name)
            .create()
            .into_future()
            .await
            .change_to_object_store_context()?;
        Ok(())
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        options: GetOptions,
    ) -> Result<(ObjectETag, Bytes), ObjectStoreError> {
        self.metrics.get.add(1, &[]);

        let request = self.client.clone().blob_client(bucket, key).get();
        let request = if let Some(etag) = options.etag {
            request.if_match(IfMatchCondition::Match(etag.0))
        } else {
            request
        };

        let mut stream = request.into_stream();

        let mut output = BytesMut::new();
        let mut etag = None;

        while let Some(mut response) = stream.try_next().await.change_to_object_store_context()? {
            if etag.is_none() {
                let content = response.blob.properties.etag.as_ref().to_string();
                etag = Some(ObjectETag(content));
            }

            while let Some(data) = response
                .data
                .try_next()
                .await
                .change_to_object_store_context()?
            {
                output.extend_from_slice(&data);
            }
        }

        let etag = etag
            .ok_or(ObjectStoreError::Metadata)
            .attach_printable("missing etag")?;

        Ok((etag, output.freeze()))
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<ObjectETag, ObjectStoreError> {
        self.metrics.put.add(1, &[]);

        let request = self
            .client
            .clone()
            .blob_client(bucket, key)
            .put_block_blob(body);
        let request = match options.mode {
            PutMode::Overwrite => request,
            PutMode::Create => request.if_match(IfMatchCondition::NotMatch("*".to_string())),
            PutMode::Update(etag) => request.if_match(IfMatchCondition::Match(etag.0)),
        };
        let response = request
            .into_future()
            .await
            .change_to_object_store_context()?;
        Ok(response.etag.into())
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<String>, ObjectStoreError> {
        self.metrics.list.add(1, &[]);

        let mut response = self
            .client
            .clone()
            .container_client(bucket)
            .list_blobs()
            .prefix(prefix.to_string())
            .into_stream();

        let mut object_ids = Vec::new();
        while let Some(response) = response.try_next().await.change_to_object_store_context()? {
            for blob in response.blobs.blobs() {
                object_ids.push(blob.name.to_string());
            }
        }

        Ok(object_ids)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        _options: DeleteOptions,
    ) -> Result<(), ObjectStoreError> {
        self.metrics.delete.add(1, &[]);

        self.client
            .clone()
            .blob_client(bucket, key)
            .delete()
            .await
            .change_to_object_store_context()?;
        Ok(())
    }
}

impl<T> ToObjectStoreResult for std::result::Result<T, azure_storage::Error> {
    type Ok = T;

    fn change_to_object_store_context(self) -> Result<T, ObjectStoreError> {
        use azure_storage::ErrorKind;

        match self {
            Ok(value) => Ok(value),
            Err(err) => match err.kind() {
                ErrorKind::HttpResponse { status, .. } => {
                    let status_code: u16 = (*status).into();
                    match status_code {
                        409 | 412 => Err(err).change_context(ObjectStoreError::Precondition),
                        304 => Err(err).change_context(ObjectStoreError::NotModified),
                        404 => Err(err).change_context(ObjectStoreError::NotFound),
                        _ => Err(err).change_context(ObjectStoreError::Request),
                    }
                }
                _ => Err(err).change_context(ObjectStoreError::Request),
            },
        }
    }
}
