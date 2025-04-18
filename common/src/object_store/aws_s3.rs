use bytes::Bytes;
use error_stack::{Result, ResultExt};

use super::{
    error::ObjectStoreError, metrics::ObjectStoreMetrics, DeleteOptions, GetOptions, ObjectETag,
    ObjectStoreResultExt, PutMode, PutOptions, ToObjectStoreResult,
};

#[derive(Clone)]
pub struct AwsS3Client {
    client: aws_sdk_s3::Client,
    metrics: ObjectStoreMetrics,
}

impl AwsS3Client {
    pub fn new_from_config(config: aws_sdk_s3::Config) -> Self {
        let client = aws_sdk_s3::Client::from_conf(config);
        Self {
            client,
            metrics: ObjectStoreMetrics::default(),
        }
    }

    pub fn new(config: aws_config::SdkConfig) -> Self {
        Self::new_from_config((&config).into())
    }

    pub async fn new_from_env() -> Self {
        let config = aws_config::load_from_env().await;
        Self::new(config)
    }

    pub async fn has_bucket(&self, name: &str) -> Result<bool, ObjectStoreError> {
        match self
            .client
            .head_bucket()
            .bucket(name)
            .send()
            .await
            .change_to_object_store_context()
        {
            Ok(_) => Ok(true),
            Err(err) if err.is_not_found() => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub async fn create_bucket(&self, name: &str) -> Result<(), ObjectStoreError> {
        self.client
            .create_bucket()
            .bucket(name)
            .send()
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

        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .customize()
            .mutate_request(move |request| {
                if let Some(etag) = &options.etag {
                    request.headers_mut().insert("If-Match", etag.0.clone());
                }
            })
            .send()
            .await
            .change_to_object_store_context()?;

        let etag = response
            .e_tag
            .ok_or(ObjectStoreError::Metadata)
            .attach_printable("missing etag")?
            .into();

        let body = response
            .body
            .collect()
            .await
            .change_context(ObjectStoreError::Request)
            .attach_printable("failed to read object body")?
            .into_bytes();

        Ok((etag, body))
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<ObjectETag, ObjectStoreError> {
        self.metrics.put.add(1, &[]);

        let response = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .customize()
            .mutate_request(move |request| match &options.mode {
                PutMode::Overwrite => {}
                PutMode::Create => {
                    // If-None-Match: "*" seems to be better supported than If-Match: "".
                    request.headers_mut().insert("If-None-Match", "*");
                }
                PutMode::Update(etag) => {
                    request.headers_mut().insert("If-Match", etag.0.clone());
                }
            })
            .send()
            .await
            .change_to_object_store_context()?;

        let etag = response
            .e_tag
            .ok_or(ObjectStoreError::Metadata)
            .attach_printable("missing etag")?
            .into();

        Ok(etag)
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<String>, ObjectStoreError> {
        self.metrics.list.add(1, &[]);

        let response = self
            .client
            .list_objects()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .change_to_object_store_context()?;

        let mut object_ids = Vec::default();
        for object in response.contents() {
            let object_id = object
                .key()
                .ok_or(ObjectStoreError::Metadata)
                .attach_printable("missing key")?;
            object_ids.push(object_id.to_string());
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
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .change_to_object_store_context()?;
        Ok(())
    }
}
