use std::time::Duration;

use apibara_etcd::{AuthOptions, EtcdClient, EtcdClientError, EtcdClientOptions};
use aws_config::{meta::region::RegionProviderChain, Region};
use azure_storage::{CloudLocation, StorageCredentials};
use clap::Args;
use error_stack::{Result, ResultExt};

use crate::{
    compaction::CompactionArgs,
    file_cache::FileCacheArgs,
    ingestion::IngestionArgs,
    object_store::{
        AwsS3Client, AzureBlobClient, ObjectStore, ObjectStoreError, ObjectStoreOptions,
    },
    server::ServerArgs,
};

#[derive(Args, Debug)]
pub struct StartArgs {
    #[clap(flatten)]
    pub object_store: ObjectStoreArgs,
    #[clap(flatten)]
    pub etcd: EtcdArgs,
    #[clap(flatten)]
    pub ingestion: IngestionArgs,
    #[clap(flatten)]
    pub compaction: CompactionArgs,
    #[clap(flatten)]
    pub server: ServerArgs,
    #[clap(flatten)]
    pub cache: FileCacheArgs,
}

#[derive(Args, Clone, Debug)]
pub struct ObjectStoreArgs {
    /// The S3 backend to use. One of `s3` or `azure-blob`.
    #[arg(long = "s3.backend", env = "DNA_S3_BACKEND", default_value = "s3")]
    pub s3_backend: String,
    /// The S3 bucket to use.
    #[arg(long = "s3.bucket", env = "DNA_S3_BUCKET")]
    pub s3_bucket: String,
    /// Under which prefix to store the data.
    #[arg(long = "s3.prefix", env = "DNA_S3_PREFIX")]
    pub s3_prefix: Option<String>,
    /// The S3 endpoint URL.
    #[arg(long = "s3.endpoint", env = "DNA_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    /// The S3 region.
    #[arg(long = "s3.region", env = "DNA_S3_REGION")]
    pub s3_region: Option<String>,
}

#[derive(Args, Clone, Debug)]
pub struct EtcdArgs {
    /// The etcd endpoints.
    #[arg(
        long = "etcd.endpoints",
        env = "DNA_ETCD_ENDPOINTS",
        value_delimiter = ',',
        num_args = 1..,
    )]
    pub etcd_endpoints: Vec<String>,
    /// The etcd prefix.
    #[arg(long = "etcd.prefix", env = "DNA_ETCD_PREFIX")]
    pub etcd_prefix: Option<String>,
    /// The etcd username.
    #[arg(long = "etcd.user", env = "DNA_ETCD_USER")]
    pub etcd_user: Option<String>,
    /// The etcd password.
    #[arg(long = "etcd.password", env = "DNA_ETCD_PASSWORD")]
    pub etcd_password: Option<String>,
    /// The etcd auth token TTL in secondls.
    #[arg(
        long = "etcd.auth-token-ttl",
        env = "DNA_ETCD_AUTH_TOKEN_TTL",
        default_value = "300"
    )]
    pub etcd_auth_token_ttl: u64,
}

impl ObjectStoreArgs {
    pub async fn into_object_store_client(self) -> Result<ObjectStore, ObjectStoreError> {
        if self.s3_backend == "azure-blob" {
            self.into_azure_blob_object_store_client().await
        } else {
            Ok(self.into_s3_object_store_client().await)
        }
    }

    pub async fn into_s3_object_store_client(self) -> ObjectStore {
        let mut config = aws_config::from_env();

        if let Some(region) = self.s3_region.as_ref() {
            let region = Region::new(region.clone());
            let region = RegionProviderChain::default_provider().or_else(region.clone());

            config = config.region(region);
        }

        if let Some(endpoint_url) = self.s3_endpoint.as_ref() {
            config = config.endpoint_url(endpoint_url.clone());
        }

        let sdk_config = config.load().await;
        let s3_config = aws_sdk_s3::Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build();

        let options = ObjectStoreOptions {
            bucket: self.s3_bucket,
            prefix: self.s3_prefix,
        };

        let s3_client = AwsS3Client::new_from_config(s3_config);
        ObjectStore::new_s3(s3_client, options)
    }

    pub async fn into_azure_blob_object_store_client(
        self,
    ) -> Result<ObjectStore, ObjectStoreError> {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_ID")
            .change_context(ObjectStoreError::Configuration)
            .attach_printable("AZURE_STORAGE_ACCOUNT_ID environment variable is required")?;

        let credentials = if let Ok(access_key) = std::env::var("AZURE_STORAGE_ACCESS_KEY") {
            StorageCredentials::access_key(account.clone(), access_key)
        } else {
            let identity = azure_identity::create_credential()
                .change_context(ObjectStoreError::Configuration)
                .attach_printable("failed to create Azure Identity provider")?;
            StorageCredentials::token_credential(identity)
        };

        let location = if let Some(endpoint_url) = self.s3_endpoint {
            CloudLocation::Custom {
                account,
                uri: endpoint_url,
            }
        } else {
            CloudLocation::Public { account }
        };

        let options = ObjectStoreOptions {
            bucket: self.s3_bucket,
            prefix: self.s3_prefix,
        };

        let client = AzureBlobClient::new(location, credentials);
        Ok(ObjectStore::new_azure_blob(client, options))
    }
}

impl EtcdArgs {
    pub async fn into_etcd_client(self) -> Result<EtcdClient, EtcdClientError> {
        let auth = if let (Some(user), Some(password)) = (self.etcd_user, self.etcd_password) {
            let token_ttl = Duration::from_secs(self.etcd_auth_token_ttl);
            Some(AuthOptions {
                user,
                password,
                token_ttl,
            })
        } else {
            None
        };

        let options = EtcdClientOptions {
            prefix: self.etcd_prefix,
            auth,
        };

        EtcdClient::connect(self.etcd_endpoints, options).await
    }
}
