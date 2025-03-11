use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::config::Credentials;
use azure_storage::{CloudLocation, StorageCredentials};
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

pub struct Azurite;

pub trait AzuriteExt {
    fn credentials(&self) -> StorageCredentials;
    fn location(&self) -> impl Future<Output = CloudLocation>;
}

impl Image for Azurite {
    fn name(&self) -> &str {
        "mcr.microsoft.com/azure-storage/azurite"
    }

    fn tag(&self) -> &str {
        "latest"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        Vec::default()
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<std::borrow::Cow<'_, str>>> {
        vec![
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            "10000",
        ]
    }
}

pub fn minio_container() -> MinIO {
    MinIO
}

pub fn azurite_container() -> Azurite {
    Azurite
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

impl AzuriteExt for ContainerAsync<Azurite> {
    fn credentials(&self) -> StorageCredentials {
        StorageCredentials::emulator()
    }

    async fn location(&self) -> CloudLocation {
        let port = self
            .get_host_port_ipv4(10_000)
            .await
            .expect("Azurite blob storage port 10000");
        CloudLocation::Emulator {
            address: "localhost".to_string(),
            port,
        }
    }
}
