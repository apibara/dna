use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use apibara_sink_webhook::{SinkWebhookConfiguration, SinkWebhookError, WebhookSink};
use http::HeaderMap;
use serde_json::{json, Value};

fn new_batch(size: usize) -> Value {
    let mut batch = Vec::new();
    for i in 0..size {
        batch.push(json!({
            "batchNum": i,
            "batchStr": format!("batch_{}", i),
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

    for i in 0..5 {
        let cursor = Some(new_cursor(i));
        let end_cursor = new_cursor(i + 1);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(5);

        sink.handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), i as usize + 1);
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
        assert_eq!(requests.len(), i as usize + 1);
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

    for i in 0..5 {
        let cursor = new_cursor(i);
        let end_cursor = new_cursor(i + 1);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(5);

        sink.handle_data(&Some(cursor), &end_cursor, &finality, &batch)
            .await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), i as usize + 1);
        assert_eq!(requests[0].body_json::<Value>()?, batch);
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
