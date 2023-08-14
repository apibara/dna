use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use apibara_sink_webhook::{SinkWebhookConfiguration, SinkWebhookError, WebhookSink};
use http::HeaderMap;
use serde_json::{json, Value};

fn new_batch(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> Value {
    let mut batch = Vec::new();

    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key,
        None => 0,
    };

    let end_block_num = end_cursor.order_key;

    for i in start_block_num..end_block_num {
        batch.push(json!({
            "block_num": i,
            "block_str": format!("block_{}", i),
        }));
    }
    json!(batch)
}

fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

#[tokio::test]
async fn test_handle_data() -> Result<(), SinkWebhookError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server.uri().parse()?,
        headers: HeaderMap::new(),
        raw: false,
    };

    let mut sink = WebhookSink::new(config);

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);

        sink.handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len() as u64, order_key + 1);
        assert_eq!(
            requests.last().unwrap().body_json::<Value>()?,
            json!({
                "data": {
                    "cursor": &cursor,
                    "end_cursor": &end_cursor,
                    "finality": &finality,
                    "batch": &batch,
                },
            })
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate() -> Result<(), SinkWebhookError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server.uri().parse()?,
        headers: HeaderMap::new(),
        raw: false,
    };

    let mut sink = WebhookSink::new(config);

    for i in 0..5 {
        let cursor = Some(new_cursor(i));

        sink.handle_invalidate(&cursor).await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len() as u64, i + 1);
        assert_eq!(
            requests.last().unwrap().body_json::<Value>()?,
            json!({
                "invalidate": {
                    "cursor": &cursor,
                }
            })
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_data_raw() -> Result<(), SinkWebhookError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server.uri().parse()?,
        headers: HeaderMap::new(),
        raw: true,
    };

    let mut sink = WebhookSink::new(config);

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);

        sink.handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len() as u64, order_key + 1);
        assert_eq!(requests.last().unwrap().body_json::<Value>()?, batch);
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate_raw() -> Result<(), SinkWebhookError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server.uri().parse()?,
        headers: HeaderMap::new(),
        raw: true,
    };

    let mut sink = WebhookSink::new(config);

    for i in 0..5 {
        let cursor = Some(new_cursor(i));

        sink.handle_invalidate(&cursor).await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 0);
    }

    Ok(())
}
