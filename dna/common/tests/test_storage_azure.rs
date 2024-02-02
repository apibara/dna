use apibara_dna_common::storage::{AzureStorageBackend, AzureStorageBackendBuilder};
use nanoid::nanoid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

pub fn new_azure_storage_backend() -> AzureStorageBackend {
    AzureStorageBackendBuilder::new()
        .with_container_name("test")
        .with_account(EMULATOR_ACCOUNT_NAME)
        .with_access_key(EMULATOR_ACCOUNT_KEY)
        .with_allow_http(true)
        .with_endpoint(format!("http://127.0.0.1:10000/{}", EMULATOR_ACCOUNT_NAME))
        .build()
        .unwrap()
}

// #[tokio::test]
pub async fn test_put_and_get_blob() {
    let mut storage = new_azure_storage_backend();
    let folder = folder_id();

    let exists = storage.exists(&folder, "test.txt").await.unwrap();
    assert!(!exists);

    let mut writer = storage.put(&folder, "test.txt").await.unwrap();
    writer.write_all(b"hello world").await.unwrap();
    writer.flush().await.unwrap();
    writer.shutdown().await.unwrap();

    let exists = storage.exists(&folder, "test.txt").await.unwrap();
    assert!(exists);

    let mut reader = storage.get(&folder, "test.txt").await.unwrap();
    let mut buf = String::new();
    reader.read_to_string(&mut buf).await.unwrap();
    assert_eq!(buf, "hello world");
}
