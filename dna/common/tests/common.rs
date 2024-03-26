use azure_storage::{CloudLocation, StorageCredentials};
use azure_storage_blobs::prelude::ClientBuilder;
use nanoid::nanoid;

use apibara_dna_common::storage::{AzureStorageBackend, AzureStorageBackendBuilder};
use testcontainers::{core::WaitFor, Image};

// Only lower-case letters are allowed in container paths.
pub const FOLDER_SAFE: [char; 26] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z',
];

pub fn folder_id() -> String {
    let id = nanoid!(10, &FOLDER_SAFE);
    format!("test-{id}")
}

/// The well-known account key used by Azurite and the legacy Azure Storage Emulator.
/// <https://docs.microsoft.com/azure/storage/common/storage-use-azurite#well-known-storage-account-and-key>
pub const EMULATOR_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

pub const EMULATOR_ACCOUNT_NAME: &str = "devstoreaccount1";

pub async fn new_azure_storage_backend(port: u16) -> AzureStorageBackend {
    let container_name = folder_id();

    create_azure_storage_container(port, container_name.clone()).await;

    AzureStorageBackendBuilder::new()
        .with_container_name(container_name)
        .with_account(EMULATOR_ACCOUNT_NAME)
        .with_access_key(EMULATOR_ACCOUNT_KEY)
        .with_allow_http(true)
        .with_endpoint(format!(
            "http://127.0.0.1:{}/{}",
            port, EMULATOR_ACCOUNT_NAME
        ))
        .build()
        .unwrap()
}

pub async fn create_azure_storage_container(port: u16, container_name: impl Into<String>) {
    let credentials = StorageCredentials::emulator();
    let location = CloudLocation::Emulator {
        address: "127.0.0.1".to_string(),
        port,
    };
    let client = ClientBuilder::with_location(location, credentials);
    let containers = client.container_client(container_name.into());
    containers.create().await.unwrap();
}

#[derive(Default)]
pub struct Azurite {}

impl Image for Azurite {
    type Args = ();

    fn name(&self) -> String {
        "mcr.microsoft.com/azure-storage/azurite".to_string()
    }

    fn tag(&self) -> String {
        "latest".to_string()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Azurite Blob service")]
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![10000, 10001, 10002]
    }
}
