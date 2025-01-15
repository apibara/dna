use apibara_etcd::normalize_prefix;
use aws_sdk_s3::{config::http::HttpResponse, error::SdkError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use error_stack::{Report, Result, ResultExt};
use tracing::debug;

#[derive(Debug)]
pub enum ObjectStoreError {
    /// Precondition failed.
    Precondition,
    /// Not modified.
    NotModified,
    /// Not found.
    NotFound,
    /// Request error.
    Request,
    /// Metadata is missing.
    Metadata,
    /// Checksum mismatch.
    ChecksumMismatch,
}

/// Options for the object store.
#[derive(Default, Clone, Debug)]
pub struct ObjectStoreOptions {
    /// The S3 bucket to use.
    pub bucket: String,
    /// Under which prefix to store the data.
    pub prefix: Option<String>,
}

/// This is an opinionated object store client.
#[derive(Clone)]
pub struct ObjectStore {
    client: aws_sdk_s3::Client,
    prefix: String,
    bucket: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectETag(pub String);

#[derive(Default, Clone, Debug)]
pub struct GetOptions {
    /// If the object exists, check that the ETag matches.
    pub etag: Option<ObjectETag>,
}

/// How to put an object.
#[derive(Clone, Debug)]
pub enum PutMode {
    /// Overwrite the object if it exists.
    Overwrite,
    /// Create the object only if it doesn't exist.
    Create,
    /// Update the object only if it exists and the ETag matches.
    Update(ObjectETag),
}

#[derive(Default, Clone, Debug)]
pub struct PutOptions {
    pub mode: PutMode,
}

#[derive(Default, Clone, Debug)]
pub struct DeleteOptions {}

#[derive(Debug)]
pub struct GetResult {
    pub body: Bytes,
    pub etag: ObjectETag,
}

#[derive(Debug)]
pub struct PutResult {
    pub etag: ObjectETag,
}

#[derive(Debug)]
pub struct DeleteResult;

impl ObjectStore {
    pub fn new(config: aws_config::SdkConfig, options: ObjectStoreOptions) -> Self {
        Self::new_from_config((&config).into(), options)
    }

    pub fn new_from_config(config: aws_sdk_s3::Config, options: ObjectStoreOptions) -> Self {
        let client = aws_sdk_s3::Client::from_conf(config);

        let prefix = normalize_prefix(options.prefix);

        Self {
            client,
            bucket: options.bucket,
            prefix,
        }
    }

    pub async fn new_from_env(options: ObjectStoreOptions) -> Self {
        let config = aws_config::load_from_env().await;
        Self::new(config, options)
    }

    /// Ensure the currently configured bucket exists.
    pub async fn ensure_bucket(&self) -> Result<(), ObjectStoreError> {
        self.client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .change_to_object_store_context()
            .attach_printable("failed to create bucket")
            .attach_printable_lazy(|| format!("bucket name: {}", self.bucket))?;
        Ok(())
    }

    #[tracing::instrument(name = "object_store_get", skip(self, options))]
    pub async fn get(
        &self,
        path: &str,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let key = self.full_key(path);
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .customize()
            .mutate_request(move |request| {
                if let Some(etag) = &options.etag {
                    request.headers_mut().insert("If-Match", etag.0.clone());
                }
            })
            .send()
            .await
            .change_to_object_store_context()
            .attach_printable("failed to get object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

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
            .attach_printable("failed to read object body")?;

        let decompressed = BytesMut::with_capacity(body.remaining());
        let mut writer = decompressed.writer();
        zstd::stream::copy_decode(&mut body.reader(), &mut writer)
            .change_context(ObjectStoreError::Request)?;
        let decompressed = writer.into_inner();

        let checksum = (&decompressed[decompressed.len() - 4..]).get_u32();
        let data = decompressed[..decompressed.len() - 4].as_ref();

        if crc32fast::hash(data) != checksum {
            return Err(ObjectStoreError::ChecksumMismatch)
                .attach_printable("checksum mismatch")
                .attach_printable_lazy(|| format!("key: {key}"));
        }

        let body = Bytes::copy_from_slice(data);

        Ok(GetResult { body, etag })
    }

    #[tracing::instrument(name = "object_store_put", skip_all, fields(key, compression_ratio))]
    pub async fn put(
        &self,
        path: &str,
        body: Bytes,
        options: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        let current_span = tracing::Span::current();

        let key = self.full_key(path);
        let size_before = body.len();

        let checksum = crc32fast::hash(&body);
        let mut body = BytesMut::from(body);
        body.put_u32(checksum);

        let mut compressed = BytesMut::with_capacity(body.len()).writer();
        zstd::stream::copy_encode(body.reader(), &mut compressed, 0)
            .change_context(ObjectStoreError::Request)?;
        let compressed = compressed.into_inner();

        let size_after = compressed.len();
        let compression_ratio = size_before as f64 / size_after as f64;

        current_span.record("key", &key);
        current_span.record("compression_ratio", compression_ratio);
        debug!(compression_ratio, key, "compressed object");

        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(compressed.freeze().into())
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
            .change_to_object_store_context()
            .attach_printable("failed to put object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

        let etag = response
            .e_tag
            .ok_or(ObjectStoreError::Metadata)
            .attach_printable("missing etag")?
            .into();

        Ok(PutResult { etag })
    }

    #[tracing::instrument(name = "object_store_delete", skip(self, _options))]
    pub async fn delete(
        &self,
        path: &str,
        _options: DeleteOptions,
    ) -> Result<DeleteResult, ObjectStoreError> {
        let key = self.full_key(path);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .change_to_object_store_context()
            .attach_printable("failed to delete object")
            .attach_printable_lazy(|| format!("key: {key}"))?;

        Ok(DeleteResult)
    }

    fn full_key(&self, path: &str) -> String {
        format!("{}{}", self.prefix, path)
    }
}

impl error_stack::Context for ObjectStoreError {}

impl std::fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectStoreError::Precondition => write!(f, "object store: precondition failed"),
            ObjectStoreError::NotModified => write!(f, "object store: not modified"),
            ObjectStoreError::NotFound => write!(f, "object store: not found"),
            ObjectStoreError::Request => write!(f, "object store: request error"),
            ObjectStoreError::Metadata => write!(f, "object store: metadata is missing or invalid"),
            ObjectStoreError::ChecksumMismatch => write!(f, "object store: checksum mismatch"),
        }
    }
}

pub trait ObjectStoreResultExt {
    fn is_precondition(&self) -> bool;
    fn is_not_modified(&self) -> bool;
    fn is_not_found(&self) -> bool;
}

impl ObjectStoreResultExt for Report<ObjectStoreError> {
    fn is_precondition(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::Precondition)
    }

    fn is_not_modified(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::NotModified)
    }

    fn is_not_found(&self) -> bool {
        matches!(self.current_context(), ObjectStoreError::NotFound)
    }
}

impl From<String> for ObjectETag {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Default for PutMode {
    fn default() -> Self {
        Self::Overwrite
    }
}

trait ToObjectStoreResult: Sized {
    type Ok;

    fn change_to_object_store_context(self) -> Result<Self::Ok, ObjectStoreError>;
}

impl<T, E> ToObjectStoreResult for std::result::Result<T, SdkError<E, HttpResponse>>
where
    SdkError<E, HttpResponse>: error_stack::Context,
{
    type Ok = T;

    fn change_to_object_store_context(self) -> Result<T, ObjectStoreError> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => match err.raw_response().map(|r| r.status().as_u16()) {
                Some(412) => Err(err).change_context(ObjectStoreError::Precondition),
                Some(304) => Err(err).change_context(ObjectStoreError::NotModified),
                Some(404) => Err(err).change_context(ObjectStoreError::NotFound),
                _ => Err(err).change_context(ObjectStoreError::Request),
            },
        }
    }
}

pub mod testing {
    use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
    use aws_sdk_s3::config::Credentials;
    use futures::Future;
    use testcontainers::{core::WaitFor, ContainerAsync, Image};

    pub struct MinIO;

    pub trait MinIOExt {
        fn s3_config(&self) -> impl Future<Output = aws_sdk_s3::Config> + Send;
    }

    impl Image for MinIO {
        fn name(&self) -> &str {
            "minio/minio"
        }

        fn tag(&self) -> &str {
            "latest"
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            Vec::default()
        }

        fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
            vec!["server", "/data"]
        }
    }

    pub fn minio_container() -> MinIO {
        MinIO
    }

    impl MinIOExt for ContainerAsync<MinIO> {
        async fn s3_config(&self) -> aws_sdk_s3::Config {
            let port = self
                .get_host_port_ipv4(9000)
                .await
                .expect("MinIO port 9000");
            s3_config_at_port(port).await
        }
    }

    pub async fn s3_config_at_port(port: u16) -> aws_sdk_s3::Config {
        let endpoint = format!("http://localhost:{}", port);
        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "test");

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .load()
            .await;

        let config: aws_sdk_s3::Config = (&config).into();
        config.to_builder().force_path_style(true).build()
    }
}
