//! Integration tests for the etcd persistence client.

mod common;

use std::future::Future;
use std::time::Duration;

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{EtcdPersistence, PersistenceClient};
use common::Etcd;
use testcontainers::clients;
use tokio::time::{timeout as tokio_timeout, Timeout};

fn timeout<F>(fut: F) -> Timeout<F>
where
    F: Future,
{
    tokio_timeout(Duration::from_secs(2), fut)
}

#[tokio::test]
#[ignore]
async fn test_single_indexer() {
    let docker = clients::Cli::default();
    let etcd = docker.run(Etcd::default());
    let etcd_port = etcd.get_host_port_ipv4(2379);
    let etcd_url = format!("http://localhost:{}", etcd_port);

    let mut persistence = EtcdPersistence::connect(&etcd_url, "test-sink")
        .await
        .unwrap();

    let cursor = persistence.get_cursor().await.unwrap();
    assert!(cursor.is_none());

    let new_cursor = Cursor {
        order_key: 123,
        unique_key: vec![1, 2, 3],
    };

    persistence.put_cursor(new_cursor.clone()).await.unwrap();
    let cursor = persistence.get_cursor().await.unwrap();
    assert_eq!(cursor, Some(new_cursor));

    persistence.delete_cursor().await.unwrap();
    let cursor = persistence.get_cursor().await.unwrap();
    assert!(cursor.is_none());
}

#[tokio::test]
#[ignore]
async fn test_multiple_indexers() {
    let docker = clients::Cli::default();
    let etcd = docker.run(Etcd::default());
    let etcd_port = etcd.get_host_port_ipv4(2379);
    let etcd_url = format!("http://localhost:{}", etcd_port);

    let mut first = EtcdPersistence::connect(&etcd_url, "first-sink")
        .await
        .unwrap();
    let mut second = EtcdPersistence::connect(&etcd_url, "second-sink")
        .await
        .unwrap();

    let first_cursor = Cursor {
        order_key: 123,
        unique_key: vec![1, 2, 3],
    };
    let second_cursor = Cursor {
        order_key: 789,
        unique_key: vec![7, 8, 9],
    };

    first.put_cursor(first_cursor.clone()).await.unwrap();
    let cursor = second.get_cursor().await.unwrap();
    assert!(cursor.is_none());

    second.put_cursor(second_cursor.clone()).await.unwrap();
    let cursor = first.get_cursor().await.unwrap();
    assert_eq!(cursor, Some(first_cursor));

    first.delete_cursor().await.unwrap();
    let cursor = second.get_cursor().await.unwrap();
    assert_eq!(cursor, Some(second_cursor));

    second.delete_cursor().await.unwrap();
    let cursor = second.get_cursor().await.unwrap();
    assert!(cursor.is_none());
}

#[tokio::test]
#[ignore]
async fn test_lock_unlock_single() {
    let docker = clients::Cli::default();
    let etcd = docker.run(Etcd::default());
    let etcd_port = etcd.get_host_port_ipv4(2379);
    let etcd_url = format!("http://localhost:{}", etcd_port);

    let mut first = EtcdPersistence::connect(&etcd_url, "first-sink")
        .await
        .unwrap();

    timeout(first.lock()).await.unwrap().unwrap();
    assert!(timeout(first.lock()).await.is_err());
    timeout(first.unlock()).await.unwrap().unwrap();
    timeout(first.lock()).await.unwrap().unwrap();
}

#[tokio::test]
#[ignore]
async fn test_lock_unlock_multiple() {
    let docker = clients::Cli::default();
    let etcd = docker.run(Etcd::default());
    let etcd_port = etcd.get_host_port_ipv4(2379);
    let etcd_url = format!("http://localhost:{}", etcd_port);

    let mut first = EtcdPersistence::connect(&etcd_url, "first-sink")
        .await
        .unwrap();
    let mut second = EtcdPersistence::connect(&etcd_url, "second-sink")
        .await
        .unwrap();

    timeout(first.lock()).await.unwrap().unwrap();
    timeout(second.lock()).await.unwrap().unwrap();

    timeout(first.unlock()).await.unwrap().unwrap();
    assert!(timeout(second.lock()).await.is_err());

    timeout(first.lock()).await.unwrap().unwrap();
}
