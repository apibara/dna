mod common;

use apibara_core::{node::v1alpha2::Cursor, starknet::v1alpha2::Filter};
use apibara_sink_common::{
    persistence::common::PersistenceClient, PersistedState, RedisPersistence,
};
use testcontainers::{clients, core::WaitFor, GenericImage};

pub fn new_redis_image() -> GenericImage {
    GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379)
        .with_wait_for(WaitFor::message_on_stdout(
            "Ready to accept connections tcp",
        ))
}

#[tokio::test]
async fn test_single_indexer() {
    let docker = clients::Cli::default();
    let redis = docker.run(new_redis_image());
    let redis_port = redis.get_host_port_ipv4(6379);
    let redis_url = format!("redis://localhost:{}", redis_port);

    let mut persistence = RedisPersistence::connect(&redis_url, "test-sink")
        .await
        .unwrap();

    let state = persistence.get_state::<Filter>().await.unwrap();
    assert!(state.cursor.is_none());

    let new_cursor = Cursor {
        order_key: 123,
        unique_key: vec![1, 2, 3],
    };
    let new_state = PersistedState::<Filter>::with_cursor(new_cursor.clone());

    persistence.put_state(new_state).await.unwrap();
    let state = persistence.get_state::<Filter>().await.unwrap();
    assert_eq!(state.cursor, Some(new_cursor));

    persistence.delete_state().await.unwrap();
    let state = persistence.get_state::<Filter>().await.unwrap();
    assert!(state.cursor.is_none());
}

#[tokio::test]
async fn test_multiple_indexers() {
    let docker = clients::Cli::default();
    let redis = docker.run(new_redis_image());
    let redis_port = redis.get_host_port_ipv4(6379);
    let redis_url = format!("redis://localhost:{}", redis_port);

    let mut first = RedisPersistence::connect(&redis_url, "first-sink")
        .await
        .unwrap();

    let mut second = RedisPersistence::connect(&redis_url, "second-sink")
        .await
        .unwrap();

    let first_cursor = Cursor {
        order_key: 123,
        unique_key: vec![1, 2, 3],
    };
    let first_state = PersistedState::<Filter>::with_cursor(first_cursor.clone());

    let second_cursor = Cursor {
        order_key: 789,
        unique_key: vec![7, 8, 9],
    };
    let second_state = PersistedState::<Filter>::with_cursor(second_cursor.clone());

    first.put_state(first_state).await.unwrap();
    let state = second.get_state::<Filter>().await.unwrap();
    assert!(state.cursor.is_none());

    second.put_state(second_state).await.unwrap();
    let state = first.get_state::<Filter>().await.unwrap();
    assert_eq!(state.cursor, Some(first_cursor));

    first.delete_state().await.unwrap();
    let state = second.get_state::<Filter>().await.unwrap();
    assert_eq!(state.cursor, Some(second_cursor));

    second.delete_state().await.unwrap();
    let state = second.get_state::<Filter>().await.unwrap();
    assert!(state.cursor.is_none());
}
