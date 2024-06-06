mod common;

use testcontainers::clients;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
#[ignore]
pub async fn test_put_and_get_blob() {
    let docker = clients::Cli::default();
    let azurite = docker.run(common::Azurite::default());
    let azurite_port = azurite.get_host_port_ipv4(10000);
    let mut storage = common::new_azure_storage_backend(azurite_port).await;

    let folder = common::folder_id();

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
