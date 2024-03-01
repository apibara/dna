use std::time::Duration;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{batching::Buffer, Context, CursorAction, Sink, ValueExt};
use apibara_sink_mongo::{MongoSink, SinkMongoError, SinkMongoOptions};
use error_stack::{Result, ResultExt};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use serde_json::{json, Value};
use testcontainers::clients;
use tokio::time::sleep;

mod common;
use crate::common::*;

fn new_batch(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> Value {
    new_batch_with_extra(start_cursor, end_cursor, json!({}))
}

fn new_batch_with_extra(start_cursor: &Option<Cursor>, end_cursor: &Cursor, extra: Value) -> Value {
    let mut batch = Vec::new();

    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key,
        None => 0,
    };

    let end_block_num = end_cursor.order_key;

    for i in start_block_num..end_block_num {
        let mut doc = json!({
            "block_num": i,
            "block_str": format!("block_{}", i),
        });

        doc.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone().into_iter());

        batch.push(doc);
    }

    json!(batch)
}

#[tokio::test]
#[ignore]
async fn test_handle_data() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    let mut all_docs = vec![];

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;
        assert_eq!(action, CursorAction::Persist);

        let action = sink.handle_data(&ctx, &new_not_array_of_objects()).await?;
        assert_eq!(action, CursorAction::Persist);

        let action = sink.handle_data(&ctx, &json!([])).await?;
        assert_eq!(action, CursorAction::Persist);

        // Make sure batching is actually disabled.
        assert_eq!(sink.batcher.buffer.data, Vec::<Value>::new());
        assert_eq!(sink.batcher.buffer.end_cursor, Cursor::default());
        assert!(!sink.batcher.should_flush());

        all_docs.extend(new_docs(&cursor, &end_cursor));
    }

    assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

    Ok(())
}

async fn test_handle_invalidate_all(
    invalidate_from: &Option<Cursor>,
) -> Result<(), SinkMongoError> {
    assert!(invalidate_from.is_none() || invalidate_from.clone().unwrap().order_key == 0);

    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    let mut all_docs: Vec<Document> = vec![];

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;

        assert_eq!(action, CursorAction::Persist);

        all_docs.extend(new_docs(&cursor, &end_cursor));
    }

    assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

    sink.handle_invalidate(invalidate_from).await?;
    assert_eq!(
        Vec::<Document>::new(),
        get_all_docs(sink.collection("test")?).await
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate_genesis() -> Result<(), SinkMongoError> {
    test_handle_invalidate_all(&None).await
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate_block_zero() -> Result<(), SinkMongoError> {
    test_handle_invalidate_all(&Some(new_cursor(0))).await
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    let mut all_docs: Vec<Document> = vec![];

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;

        all_docs.extend(new_docs(&cursor, &end_cursor));

        assert_eq!(action, CursorAction::Persist);
    }

    assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

    let invalidate_from = 2;

    sink.handle_invalidate(&Some(new_cursor(invalidate_from)))
        .await?;

    let expected_docs: Vec<Document> = all_docs
        .into_iter()
        .filter(|doc| {
            doc.get_document("_cursor")
                .unwrap()
                .get_i64("from")
                .unwrap() as u64
                <= invalidate_from
        })
        .collect();

    assert_eq!(expected_docs, get_all_docs(sink.collection("test")?).await);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate_with_extra_condition() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        invalidate: Some(doc! { "col1": "a", "col2": "a" }),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    let mut docs_count = 0;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        {
            let batch =
                new_batch_with_extra(&cursor, &end_cursor, json!({ "col1": "a", "col2": "a" }));
            docs_count += batch_size;
            let action = sink.handle_data(&ctx, &batch).await?;
            assert_eq!(action, CursorAction::Persist);
        }

        {
            let batch =
                new_batch_with_extra(&cursor, &end_cursor, json!({ "col1": "a", "col2": "b" }));
            docs_count += batch_size;
            let action = sink.handle_data(&ctx, &batch).await?;
            assert_eq!(action, CursorAction::Persist);
        }
    }

    assert_eq!(docs_count, batch_size * num_batches * 2);

    let invalidate_from = 2;

    sink.handle_invalidate(&Some(new_cursor(invalidate_from)))
        .await?;

    // We expect all documents for `{col1: a, col2: b}`, but only
    // one batch worth of data for `{col1: a, col2: a}`.
    let expected_docs_count = (batch_size * num_batches) + (batch_size * (invalidate_from - 1));
    assert_eq!(
        expected_docs_count,
        get_all_docs(sink.collection("test")?).await.len() as u64
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_data_in_entity_mode() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        entity_mode: Some(true),
        invalidate: None,
        batch_seconds: None,
    };

    let mut sink = MongoSink::from_options(options).await?;
    let finality = DataFinality::DataStatusFinalized;

    {
        // Insert the first batch.
        // Note that (0x1, 2) has duplicate items. Technically this is allowed but values will be
        // squashed.
        let cursor = Some(new_cursor(0));
        let end_cursor = new_cursor(1);
        let batch = json!([
            {"entity": { "address": "0x1", "token_id": "1", }, "update": { "$set": { "v0": "a", "v1": "a" } } },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": { "$set": { "v0": "b", "v1": "b" } } },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": [{ "$set": { "v0": "a", "v1": "a" } }] },
            {"entity": { "address": "0x1", "token_id": "3", }, "update": [{ "$set": { "v0": "a", "v1": "a" } }] },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    {
        // Update some values for some entities.
        let cursor = Some(new_cursor(1));
        let end_cursor = new_cursor(2);
        let batch = json!([
            {"entity": { "address": "0x1", "token_id": "1", }, "update": { "$set": { "v1": "b"}, "$inc": { "v2": 7 } } },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": [{ "$set": { "v0": "b"} }] },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        // Check that the values were updated correctly.
        // For example, we check that key v0 is still present.

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "1" }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
        assert_eq!(new_doc.get_i64("v2").unwrap(), 7);

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "2" }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
    }

    {
        // Update a single entity and insert a new one.
        let cursor = Some(new_cursor(2));
        let end_cursor = new_cursor(3);
        let batch = json!([
            { "entity": { "address": "0x1", "token_id": "1" }, "update": { "$set": { "v1": "c" } } },
            { "entity": { "address": "0x1", "token_id": "4" }, "update": { "$set": { "v0": "a", "v1": "a" } } },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        let updated_docs = sink
            .collection("test")?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "1" }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(updated_docs.len(), 1);
        let updated_doc = &updated_docs[0];
        assert_eq!(updated_doc.get_str("v0").unwrap(), "a");
        assert_eq!(updated_doc.get_str("v1").unwrap(), "c");

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "4" }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "a");
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_handle_invalidate_in_entity_mode() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        entity_mode: Some(true),
        invalidate: None,
        batch_seconds: None,
    };

    let mut sink = MongoSink::from_options(options).await?;
    let finality = DataFinality::DataStatusFinalized;

    {
        let cursor = Some(new_cursor(0));
        let end_cursor = new_cursor(1);
        let batch = json!([
            { "entity": { "address": "0x1", "token_id": "1" }, "update": { "$set": { "v0": "a", "v1": "a" } } },
            { "entity": { "address": "0x1", "token_id": "2" }, "update": { "$set": { "v0": "a", "v1": "a"} } },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    {
        let cursor = Some(new_cursor(1));
        let end_cursor = new_cursor(2);
        let batch = json!([
            json!({ "entity": { "address": "0x1", "token_id": "2" }, "update": { "$set": { "v1": "b" } } }),
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
    }

    {
        // This actually shouldn't invalidate any data since the new head is the same as before
        // (2), but it catches off by one errors in the invalidation logic.
        let new_head = Some(new_cursor(2));
        sink.handle_invalidate(&new_head).await?;

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
    }

    {
        // Now actually invalidate data.
        let new_head = Some(new_cursor(1));
        sink.handle_invalidate(&new_head).await?;

        let new_docs = sink
            .collection("test")?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkMongoError)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkMongoError)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "a");
    }

    Ok(())
}

#[tokio::test]
// #[ignore]
async fn test_handle_data_batch_mode() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let batch_seconds = 1;

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        batch_seconds: Some(batch_seconds),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 15;

    let mut all_docs = vec![];
    let mut buffer = Buffer::new();

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        // If the data is not an array of objects or an empty array,
        // we only skip if we're batching and the buffer is not empty
        let expected_action = if sink.batcher.is_batching() && !sink.batcher.is_flushed() {
            CursorAction::Skip
        } else {
            CursorAction::Persist
        };

        // Test the case where the data is not an array of objects
        let action = sink.handle_data(&ctx, &new_not_array_of_objects()).await?;
        assert_eq!(action, expected_action);

        // Test the case where the data is an empty array
        let action = sink.handle_data(&ctx, &json!([])).await?;
        assert_eq!(action, expected_action);

        // Test the regular case
        let action = sink.handle_data(&ctx, &batch).await?;
        let batch = batch.as_array_of_objects().unwrap().to_vec();

        buffer.data.extend(batch);
        buffer.end_cursor = end_cursor.clone();

        // Test batching
        if buffer.start_at.elapsed().as_secs() < batch_seconds {
            assert_eq!(buffer.data, sink.batcher.buffer.data);
            assert_eq!(buffer.end_cursor, sink.batcher.buffer.end_cursor);
            assert_eq!(action, CursorAction::Skip);
        // Test flushing
        } else {
            all_docs.extend(new_docs(
                &Some(buffer.start_cursor.clone()),
                &buffer.end_cursor,
            ));
            assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

            assert!(sink.batcher.is_flushed());
            assert_eq!(action, CursorAction::PersistAt(buffer.end_cursor.clone()));

            buffer = Buffer::new();
            buffer.start_cursor = end_cursor
        }

        // Test non finalized data should not be batched, it should be written right away
        let non_finalized_order_key = order_key * 100;

        let cursor = Some(new_cursor(non_finalized_order_key * batch_size));
        let end_cursor = new_cursor((non_finalized_order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusAccepted;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;
        assert_eq!(action, CursorAction::Persist);

        all_docs.extend(new_docs(&cursor, &end_cursor));
        assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

        sleep(Duration::from_millis(250)).await;
    }

    assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await);

    Ok(())
}

#[tokio::test]
// #[ignore]
async fn test_invalidate_batch_mode() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let batch_seconds = 1;

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
        collection_names: None,
        batch_seconds: Some(batch_seconds),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;
        assert_eq!(action, CursorAction::Skip);
    }

    assert_eq!(
        Vec::<Document>::new(),
        get_all_docs(sink.collection("test")?).await,
    );

    sleep(Duration::from_secs(batch_seconds)).await;

    assert_eq!(
        Vec::<Document>::new(),
        get_all_docs(sink.collection("test")?).await,
    );

    sink.handle_invalidate(&Some(new_cursor(num_batches * batch_size + 1)))
        .await?;

    let all_docs = new_docs(&Some(new_cursor(0)), &new_cursor(num_batches * batch_size));
    assert_eq!(all_docs, get_all_docs(sink.collection("test")?).await,);

    Ok(())
}
