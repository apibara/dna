use apibara_dna_protocol::dna::{common::Cursor, stream::DataFinality};
use apibara_sink_common::{Context, Sink, SinkError};
use apibara_sink_webhook::{SinkWebhookConfiguration, WebhookSink};
use error_stack::{Result, ResultExt};
use http::{HeaderMap, Uri};
use serde_json::{json, Number, Value};

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
#[ignore]
async fn test_handle_data() -> Result<(), SinkError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server
            .uri()
            .parse::<Uri>()
            .change_context(SinkError::Runtime)?,
        headers: HeaderMap::new(),
        raw: false,
    };

    let mut sink = WebhookSink::new(config);

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::Finalized;
        let batch = new_batch(&cursor, &end_cursor);

        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len() as u64, order_key + 1);
        assert_eq!(
            requests
                .last()
                .unwrap()
                .body_json::<Value>()
                .change_context(SinkError::Runtime)?,
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
#[ignore]
async fn test_handle_invalidate() -> Result<(), SinkError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server
            .uri()
            .parse::<Uri>()
            .change_context(SinkError::Runtime)?,
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
            requests
                .last()
                .unwrap()
                .body_json::<Value>()
                .change_context(SinkError::Runtime)?,
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
#[ignore]
async fn test_handle_data_raw() -> Result<(), SinkError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server
            .uri()
            .parse::<Uri>()
            .change_context(SinkError::Runtime)?,
        headers: HeaderMap::new(),
        raw: true,
    };

    let mut sink = WebhookSink::new(config);

    let batch_size = 2;
    let num_batches = 5;

    let mut prev_count = 0;
    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::Finalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        let batch_as_array = batch.as_array().unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len() - prev_count, batch_as_array.len());
        assert_eq!(
            &requests
                .last()
                .unwrap()
                .body_json::<Value>()
                .change_context(SinkError::Runtime)?,
            batch_as_array.last().unwrap()
        );
        prev_count = requests.len();
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate_raw() -> Result<(), SinkError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server
            .uri()
            .parse::<Uri>()
            .change_context(SinkError::Runtime)?,
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

#[tokio::test]
#[ignore]
async fn test_handle_data_skips_null_values() -> Result<(), SinkError> {
    let server = wiremock::MockServer::start().await;

    let config = SinkWebhookConfiguration {
        target_url: server
            .uri()
            .parse::<Uri>()
            .change_context(SinkError::Runtime)?,
        headers: HeaderMap::new(),
        raw: false,
    };

    let mut sink = WebhookSink::new(config);

    let cursor = Some(new_cursor(0));
    let end_cursor = new_cursor(2);
    let finality = DataFinality::Finalized;
    let ctx = Context {
        cursor: cursor.clone(),
        end_cursor: end_cursor.clone(),
        finality,
    };

    // Case 1: all values are null.
    {
        let batch = json!([Value::Null, Value::Null]);

        sink.handle_data(&ctx, &batch).await?;
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 0);
    }

    // Case 2: some values are not null, so it should be sent.
    {
        let batch = json!([Value::Null, Value::Number(Number::from_f64(123.0).unwrap())]);

        sink.handle_data(&ctx, &batch).await?;
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
    }

    Ok(())
}
