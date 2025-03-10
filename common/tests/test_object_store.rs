use testcontainers::{runners::AsyncRunner, ContainerAsync};

use apibara_dna_common::object_store::{
    testing::{self, azurite_container, minio_container, AzuriteExt, MinIOExt},
    AwsS3Client, AzureBlobClient, DeleteOptions, GetOptions, ObjectETag, ObjectStore,
    ObjectStoreClient, ObjectStoreOptions, ObjectStoreResultExt, PutMode, PutOptions,
};

async fn start_minio() -> (ContainerAsync<testing::MinIO>, ObjectStoreClient) {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;
    let client = AwsS3Client::new_from_config(config);
    (minio, client.into())
}

async fn start_azurite() -> (ContainerAsync<testing::Azurite>, ObjectStoreClient) {
    let azurite = azurite_container().start().await.unwrap();
    let client = AzureBlobClient::new(azurite.location().await, azurite.credentials());
    (azurite, client.into())
}

async fn dot_put_and_get_no_prefix_no_precondition(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );
    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    assert!(!put_res.etag.0.is_empty());

    let get_res = client.get("test", GetOptions::default()).await.unwrap();
    assert_eq!(get_res.etag, put_res.etag);
    assert_eq!(get_res.body, "Hello, World".as_bytes());
}

#[tokio::test]
async fn test_s3_put_and_get_no_prefix_no_precondition() {
    let (_minio, client) = start_minio().await;
    dot_put_and_get_no_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_azure_blob_put_and_get_no_prefix_no_precondition() {
    let (_azurite, client) = start_azurite().await;
    dot_put_and_get_no_prefix_no_precondition(client).await;
}

// Put an object in the bucket with prefix.
// Put an object with the same filename in the bucket without prefix.
// Check that they are indeed different.
async fn do_put_and_get_with_prefix_no_precondition(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner.clone(),
        ObjectStoreOptions {
            bucket: "test".to_string(),
            prefix: Some("my-prefix".to_string()),
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put("test", "With my-prefix".into(), PutOptions::default())
        .await
        .unwrap();

    {
        let client = ObjectStore::new(
            inner,
            ObjectStoreOptions {
                bucket: "test".to_string(),
                prefix: None,
            },
        );
        client
            .put("test", "Without prefix".into(), PutOptions::default())
            .await
            .unwrap();
    }

    let get_res = client.get("test", GetOptions::default()).await.unwrap();
    assert_eq!(get_res.body, "With my-prefix".as_bytes());
}

#[tokio::test]
async fn test_s3_put_and_get_with_prefix_no_precondition() {
    let (_minio, client) = start_minio().await;
    do_put_and_get_with_prefix_no_precondition(client).await;
}

#[tokio::test]
async fn test_azure_put_and_get_with_prefix_no_precondition() {
    let (_azurite, client) = start_azurite().await;
    do_put_and_get_with_prefix_no_precondition(client).await;
}

async fn do_test_get_with_etag(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    client
        .get(
            "test",
            GetOptions {
                etag: Some(put_res.etag),
            },
        )
        .await
        .unwrap();

    let response = client
        .get(
            "test",
            GetOptions {
                etag: Some(ObjectETag("bad etag".to_string())),
            },
        )
        .await;

    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());
}

#[tokio::test]
async fn test_s3_get_with_etag() {
    let (_minio, client) = start_minio().await;
    do_test_get_with_etag(client).await;
}

#[tokio::test]
async fn test_azure_get_with_etag() {
    let (_azurite, client) = start_azurite().await;
    do_test_get_with_etag(client).await;
}

async fn do_put_with_overwrite(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let put_res = client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    let original_etag = put_res.etag;

    let put_res = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Overwrite,
            },
        )
        .await
        .unwrap();

    assert_ne!(put_res.etag, original_etag);
}

#[tokio::test]
async fn test_s3_put_with_overwrite() {
    let (_minio, client) = start_minio().await;
    do_put_with_overwrite(client).await;
}

#[tokio::test]
async fn test_azure_put_with_overwrite() {
    let (_azurite, client) = start_azurite().await;
    do_put_with_overwrite(client).await;
}

async fn do_put_with_create(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put(
            "test",
            "Hello, World".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await
        .unwrap();

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await;

    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());
}

#[tokio::test]
async fn test_s3_put_with_create() {
    let (_minio, client) = start_minio().await;
    do_put_with_create(client).await;
}

#[tokio::test]
async fn test_azure_put_with_create() {
    let (_azurite, client) = start_azurite().await;
    do_put_with_create(client).await;
}

async fn do_put_with_update(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    let response = client
        .put(
            "test",
            "Hello, World".into(),
            PutOptions {
                mode: PutMode::Create,
            },
        )
        .await
        .unwrap();

    let original_etag = response.etag;

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Update("bad etag".to_string().into()),
            },
        )
        .await;
    assert!(response.is_err());
    assert!(response.unwrap_err().is_precondition());

    let response = client
        .put(
            "test",
            "Something else".into(),
            PutOptions {
                mode: PutMode::Update(original_etag.clone()),
            },
        )
        .await
        .unwrap();

    assert_ne!(response.etag, original_etag);
}

#[tokio::test]
async fn test_s3_put_with_update() {
    let (_minio, inner) = start_minio().await;
    do_put_with_update(inner).await;
}

#[tokio::test]
async fn test_azure_put_with_update() {
    let (_azurite, inner) = start_azurite().await;
    do_put_with_update(inner).await;
}

async fn do_delete(inner: ObjectStoreClient) {
    let client = ObjectStore::new(
        inner,
        ObjectStoreOptions {
            bucket: "test".to_string(),
            ..Default::default()
        },
    );

    client.ensure_bucket().await.unwrap();

    client
        .put("test", "Hello, World".into(), PutOptions::default())
        .await
        .unwrap();

    client
        .delete("test", DeleteOptions::default())
        .await
        .unwrap();

    let response = client.get("test", GetOptions::default()).await;
    assert!(response.is_err());
    assert!(response.unwrap_err().is_not_found());
}

#[tokio::test]
async fn test_s3_delete() {
    let (_minio, inner) = start_minio().await;
    do_delete(inner).await;
}

#[tokio::test]
async fn test_azure_delete() {
    let (_azurite, inner) = start_azurite().await;
    do_delete(inner).await;
}
