use apibara_etcd::{AuthOptions, EtcdClient, EtcdClientError, EtcdClientOptions};
use aws_config::{meta::region::RegionProviderChain, Region};
use clap::Args;
use error_stack::Result;

use crate::{
    compaction::CompactionArgs,
    file_cache::FileCacheArgs,
    ingestion::IngestionArgs,
    object_store::{ObjectStore, ObjectStoreOptions},
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
}

impl ObjectStoreArgs {
    pub async fn into_object_store_client(self) -> ObjectStore {
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

        ObjectStore::new_from_config(s3_config, options)
    }
}

impl EtcdArgs {
    pub async fn into_etcd_client(self) -> Result<EtcdClient, EtcdClientError> {
        let auth = if let (Some(user), Some(password)) = (self.etcd_user, self.etcd_password) {
            Some(AuthOptions { user, password })
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
