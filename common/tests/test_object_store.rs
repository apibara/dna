use testcontainers::runners::AsyncRunner;

use apibara_dna_common::object_store::{
    testing::{minio_container, MinIOExt},
    DeleteOptions, GetOptions, ObjectETag, ObjectStore, ObjectStoreOptions, ObjectStoreResultExt,
    PutMode, PutOptions,
};

#[tokio::test]
async fn test_put_and_get_no_prefix_no_precondition() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config,
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

    assert_eq!(
        put_res.etag,
        ObjectETag("\"600335e986d6c8ce1e348d20d6d16045\"".to_string())
    );

    let get_res = client.get("test", GetOptions::default()).await.unwrap();
    assert_eq!(get_res.etag, put_res.etag);
    assert_eq!(get_res.body, "Hello, World".as_bytes());
}

#[tokio::test]
async fn test_put_and_get_with_prefix_no_precondition() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    // Put an object in the bucket with prefix.
    // Put an object with the same filename in the bucket without prefix.
    // Check that they are indeed different.
    let client = ObjectStore::new_from_config(
        config.clone(),
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
        let client = ObjectStore::new_from_config(
            config,
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
async fn test_get_with_etag() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config.clone(),
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
async fn test_put_with_overwrite() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config.clone(),
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
async fn test_put_with_create() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config.clone(),
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
async fn test_put_with_update() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config.clone(),
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
async fn test_delete() {
    let minio = minio_container().start().await.unwrap();
    let config = minio.s3_config().await;

    let client = ObjectStore::new_from_config(
        config.clone(),
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
