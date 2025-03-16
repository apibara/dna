use bytes::Bytes;
use error_stack::{Result, ResultExt};

use super::{
    error::ObjectStoreError, DeleteOptions, GetOptions, ObjectETag, ObjectStoreResultExt, PutMode,
    PutOptions, ToObjectStoreResult,
};

#[derive(Clone)]
pub struct AwsS3Client(aws_sdk_s3::Client);

impl AwsS3Client {
    pub fn new_from_config(config: aws_sdk_s3::Config) -> Self {
        let client = aws_sdk_s3::Client::from_conf(config);
        Self(client)
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
            .0
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
        self.0
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
        let response = self
            .0
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
        let response = self
            .0
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

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        _options: DeleteOptions,
    ) -> Result<(), ObjectStoreError> {
        self.0
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .change_to_object_store_context()?;
        Ok(())
    }
}
