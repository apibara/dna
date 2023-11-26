#![allow(dead_code)]

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::Sink;
use apibara_sink_postgres::{InvalidateColumn, PostgresSink, SinkPostgresOptions};
use testcontainers::{core::WaitFor, GenericImage};

pub fn new_postgres_image() -> GenericImage {
    GenericImage::new("postgres", "15-alpine")
        .with_exposed_port(5432)
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
}

pub fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

pub async fn new_sink(port: u16) -> PostgresSink {
    new_sink_with_invalidate(port, None).await
}

pub async fn new_sink_with_invalidate(
    port: u16,
    invalidate: Option<Vec<InvalidateColumn>>,
) -> PostgresSink {
    let options = SinkPostgresOptions {
        connection_string: Some(format!("postgresql://postgres@localhost:{}", port)),
        table_name: Some("test".into()),
        no_tls: Some(true),
        invalidate,
        ..Default::default()
    };
    PostgresSink::from_options(options).await.unwrap()
}

pub async fn new_entity_mode_sink(port: u16) -> PostgresSink {
    let options = SinkPostgresOptions {
        connection_string: Some(format!("postgresql://postgres@localhost:{}", port)),
        table_name: Some("test".into()),
        no_tls: Some(true),
        entity_mode: Some(true),
        ..Default::default()
    };
    PostgresSink::from_options(options).await.unwrap()
}
