use apibara_etcd::{EtcdClient, EtcdClientError, EtcdClientOptions};
use aws_config::{meta::region::RegionProviderChain, Region};
use clap::Args;
use error_stack::Result;

use crate::object_store::{ObjectStore, ObjectStoreOptions};

#[derive(Args, Clone, Debug)]
pub struct ObjectStoreArgs {
    /// The S3 bucket to use.
    #[arg(long = "s3.bucket", env = "DNA_S3_BUCKET")]
    pub bucket: String,
    /// Under which prefix to store the data.
    #[arg(long = "s3.prefix", env = "DNA_S3_PREFIX")]
    pub s3_prefix: Option<String>,
    /// The S3 endpoint URL.
    #[arg(long = "s3.endpoint-url", env = "DNA_S3_ENDPOINT_URL")]
    pub endpoint_url: Option<String>,
    /// The S3 region.
    #[arg(long = "s3.region", env = "DNA_S3_REGION")]
    pub region: Option<String>,
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
}

impl ObjectStoreArgs {
    pub async fn into_object_store_client(self) -> ObjectStore {
        let mut config = aws_config::from_env();

        if let Some(region) = self.region.as_ref() {
            let region = Region::new(region.clone());
            let region = RegionProviderChain::default_provider().or_else(region.clone());

            config = config.region(region);
        }

        if let Some(endpoint_url) = self.endpoint_url.as_ref() {
            config = config.endpoint_url(endpoint_url.clone());
        }

        let sdk_config = config.load().await;
        let s3_config = aws_sdk_s3::Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build();

        let options = ObjectStoreOptions {
            bucket: self.bucket,
            prefix: self.s3_prefix,
        };

        ObjectStore::new_from_config(s3_config, options)
    }
}

impl EtcdArgs {
    pub async fn into_etcd_client(self) -> Result<EtcdClient, EtcdClientError> {
        let options = EtcdClientOptions {
            prefix: self.etcd_prefix,
        };

        EtcdClient::connect(self.etcd_endpoints, options).await
    }
}
