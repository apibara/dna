use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{Context, CursorAction, Sink, SinkError};
use apibara_sink_mongo::{MongoSink, SinkMongoOptions};
use error_stack::{Result, ResultExt};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use serde_json::{json, Value};
use testcontainers::clients;

mod common;
use crate::common::*;

fn new_batch(
    start_cursor: &Option<Cursor>,
    end_cursor: &Cursor,
    collection_names: &[String],
) -> Value {
    new_batch_with_extra(start_cursor, end_cursor, json!({}), collection_names)
}

fn new_batch_with_extra(
    start_cursor: &Option<Cursor>,
    end_cursor: &Cursor,
    extra: Value,
    collection_names: &[String],
) -> Value {
    let mut batch = Vec::new();

    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key,
        None => 0,
    };

    let end_block_num = end_cursor.order_key;

    for collection_name in collection_names {
        for i in start_block_num..end_block_num {
            let mut doc = json!({
                "block_num": i,
                "block_str": format!("block_{}", i),
            });

            doc.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone().into_iter());

            batch.push(json!({"data": doc, "collection": collection_name}));
        }
    }

    json!(batch)
}

#[tokio::test]
async fn test_handle_data() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_names: Some(collection_names.clone()),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let batch = new_batch(&cursor, &end_cursor, &collection_names);

        let action = sink.handle_data(&ctx, &batch).await?;
        assert_eq!(action, CursorAction::Persist);

        let action = sink.handle_data(&ctx, &new_not_array_of_objects()).await?;
        assert_eq!(action, CursorAction::Persist);

        let action = sink.handle_data(&ctx, &json!([])).await?;
        assert_eq!(action, CursorAction::Persist);
    }

    for collection_name in &collection_names {
        let mut all_docs = vec![];

        for order_key in 0..num_batches {
            let cursor = Some(new_cursor(order_key * batch_size));
            let end_cursor = new_cursor((order_key + 1) * batch_size);
            all_docs.extend(new_docs(&cursor, &end_cursor));
        }

        assert_eq!(
            all_docs,
            get_all_docs(sink.collection(collection_name)?).await
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_data_empty_collection() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_names: Some(collection_names.clone()),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let cursor = Some(new_cursor(0));
    let end_cursor = new_cursor(1);
    let finality = DataFinality::DataStatusFinalized;
    let ctx = Context {
        cursor: cursor.clone(),
        end_cursor: end_cursor.clone(),
        finality,
    };

    // Generate data with only test1 collection
    let batch = new_batch(&cursor, &end_cursor, &["test1".into()]);

    // handle_data shouldn't fail
    let action = sink.handle_data(&ctx, &batch).await?;
    assert_eq!(action, CursorAction::Persist);

    let all_docs = new_docs(&cursor, &end_cursor);

    assert_eq!(all_docs, get_all_docs(sink.collection("test1")?).await);

    assert_eq!(
        Vec::<Document>::new(),
        get_all_docs(sink.collection("test2")?).await,
    );

    Ok(())
}

async fn test_handle_invalidate_all(invalidate_from: &Option<Cursor>) -> Result<(), SinkError> {
    assert!(invalidate_from.is_none() || invalidate_from.clone().unwrap().order_key == 0);

    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_names: Some(collection_names.clone()),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor, &collection_names);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;

        assert_eq!(action, CursorAction::Persist);
    }

    sink.handle_invalidate(invalidate_from).await?;

    for collection_name in &collection_names {
        assert_eq!(
            Vec::<Document>::new(),
            get_all_docs(sink.collection(collection_name)?).await
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate_genesis() -> Result<(), SinkError> {
    test_handle_invalidate_all(&None).await
}

#[tokio::test]
async fn test_handle_invalidate_block_zero() -> Result<(), SinkError> {
    test_handle_invalidate_all(&Some(new_cursor(0))).await
}

#[tokio::test]
async fn test_handle_invalidate() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_names: Some(collection_names.clone()),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    for order_key in 0..num_batches {
        let cursor = Some(new_cursor(order_key * batch_size));
        let end_cursor = new_cursor((order_key + 1) * batch_size);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(&cursor, &end_cursor, &collection_names);
        let ctx = Context {
            cursor: cursor.clone(),
            end_cursor: end_cursor.clone(),
            finality,
        };

        let action = sink.handle_data(&ctx, &batch).await?;

        assert_eq!(action, CursorAction::Persist);
    }

    let invalidate_from = 2;

    sink.handle_invalidate(&Some(new_cursor(invalidate_from)))
        .await?;

    for collection_name in &collection_names {
        let mut all_docs: Vec<Document> = vec![];

        for order_key in 0..num_batches {
            let cursor = Some(new_cursor(order_key * batch_size));
            let end_cursor = new_cursor((order_key + 1) * batch_size);

            all_docs.extend(new_docs(&cursor, &end_cursor));
        }

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

        assert_eq!(
            expected_docs,
            get_all_docs(sink.collection(collection_name)?).await
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate_with_extra_condition() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_names: Some(collection_names.clone()),
        invalidate: Some(doc! { "col1": "a", "col2": "a" }),
        ..SinkMongoOptions::default()
    };

    let mut sink = MongoSink::from_options(options).await?;

    let batch_size = 2;
    let num_batches = 5;

    let num_collections = collection_names.len() as u64;
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
            let batch = new_batch_with_extra(
                &cursor,
                &end_cursor,
                json!({ "col1": "a", "col2": "a" }),
                &collection_names,
            );
            docs_count += batch_size * num_collections;
            let action = sink.handle_data(&ctx, &batch).await?;
            assert_eq!(action, CursorAction::Persist);
        }

        {
            let batch = new_batch_with_extra(
                &cursor,
                &end_cursor,
                json!({ "col1": "a", "col2": "b" }),
                &collection_names,
            );
            docs_count += batch_size * num_collections;
            let action = sink.handle_data(&ctx, &batch).await?;
            assert_eq!(action, CursorAction::Persist);
        }
    }

    assert_eq!(docs_count, batch_size * num_batches * 2 * num_collections);

    let invalidate_from = 2;

    sink.handle_invalidate(&Some(new_cursor(invalidate_from)))
        .await?;

    for collection_name in &collection_names {
        // We expect all documents for `{col1: a, col2: b}`, but only
        // one batch worth of data for `{col1: a, col2: a}`.
        let expected_docs_count = (batch_size * num_batches) + (batch_size * (invalidate_from - 1));
        assert_eq!(
            expected_docs_count,
            get_all_docs(sink.collection(collection_name)?).await.len() as u64
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_data_in_entity_mode() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: None,
        collection_names: Some(collection_names.clone()),
        entity_mode: Some(true),
        invalidate: None,
        batch_seconds: None,
    };

    let mut sink = MongoSink::from_options(options).await?;
    let finality = DataFinality::DataStatusFinalized;

    for collection_name in &collection_names {
        // Insert the first batch.
        // Note that (0x1, 2) has duplicate items. Technically this is allowed but values will be
        // squashed.
        let cursor = Some(new_cursor(0));
        let end_cursor = new_cursor(1);
        let batch = json!([
            {"entity": { "address": "0x1", "token_id": "1", }, "update": { "$set": { "v0": "a", "v1": "a" } }, "collection": collection_name },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": { "$set": { "v0": "b", "v1": "b" } }, "collection": collection_name },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": [{ "$set": { "v0": "a", "v1": "a" } }], "collection": collection_name },
            {"entity": { "address": "0x1", "token_id": "3", }, "update": [{ "$set": { "v0": "a", "v1": "a" } }], "collection": collection_name },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    for collection_name in &collection_names {
        // Update some values for some entities.
        let cursor = Some(new_cursor(1));
        let end_cursor = new_cursor(2);
        let batch = json!([
            {"entity": { "address": "0x1", "token_id": "1", }, "update": { "$set": { "v1": "b"}, "$inc": { "v2": 7 } }, "collection": collection_name },
            {"entity": { "address": "0x1", "token_id": "2", }, "update": [{ "$set": { "v0": "b"} }], "collection": collection_name },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    for collection_name in &collection_names {
        // Check that the values were updated correctly.
        // For example, we check that key v0 is still present.

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "1" }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
        assert_eq!(new_doc.get_i64("v2").unwrap(), 7);

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "2" }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
    }

    for collection_name in &collection_names {
        // Update a single entity and insert a new one.
        let cursor = Some(new_cursor(2));
        let end_cursor = new_cursor(3);
        let batch = json!([
            { "entity": { "address": "0x1", "token_id": "1" }, "update": { "$set": { "v1": "c" } }, "collection": collection_name },
            { "entity": { "address": "0x1", "token_id": "4" }, "update": { "$set": { "v0": "a", "v1": "a" } }, "collection": collection_name },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    for collection_name in &collection_names {
        let updated_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "1" }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(updated_docs.len(), 1);
        let updated_doc = &updated_docs[0];
        assert_eq!(updated_doc.get_str("v0").unwrap(), "a");
        assert_eq!(updated_doc.get_str("v1").unwrap(), "c");

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! {"_cursor.to": Bson::Null, "address": "0x1", "token_id": "4" }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "a");
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate_in_entity_mode() -> Result<(), SinkError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(new_mongo_image());
    let port = mongo.get_host_port_ipv4(27017);

    let collection_names = vec!["test1".into(), "test2".into()];

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: None,
        collection_names: Some(collection_names.clone()),
        entity_mode: Some(true),
        invalidate: None,
        batch_seconds: None,
    };

    let mut sink = MongoSink::from_options(options).await?;
    let finality = DataFinality::DataStatusFinalized;

    for collection_name in &collection_names {
        let cursor = Some(new_cursor(0));
        let end_cursor = new_cursor(1);
        let batch = json!([
            { "entity": { "address": "0x1", "token_id": "1" }, "update": { "$set": { "v0": "a", "v1": "a" } }, "collection": collection_name },
            { "entity": { "address": "0x1", "token_id": "2" }, "update": { "$set": { "v0": "a", "v1": "a"} }, "collection": collection_name },
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;
    }

    for collection_name in &collection_names {
        let cursor = Some(new_cursor(1));
        let end_cursor = new_cursor(2);
        let batch = json!([
            json!({ "entity": { "address": "0x1", "token_id": "2" }, "update": { "$set": { "v1": "b" } }, "collection": collection_name }),
        ]);

        let ctx = Context {
            cursor,
            end_cursor,
            finality,
        };

        sink.handle_data(&ctx, &batch).await?;

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
    }

    for collection_name in &collection_names {
        // This actually shouldn't invalidate any data since the new head is the same as before
        // (2), but it catches off by one errors in the invalidation logic.
        let new_head = Some(new_cursor(2));
        sink.handle_invalidate(&new_head).await?;

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "b");
    }

    for collection_name in &collection_names {
        // Now actually invalidate data.
        let new_head = Some(new_cursor(1));
        sink.handle_invalidate(&new_head).await?;

        let new_docs = sink
            .collection(collection_name)?
            .find(
                Some(doc! { "token_id": "2", "_cursor.to": Bson::Null }),
                None,
            )
            .await
            .change_context(SinkError::Runtime)?
            .try_collect::<Vec<_>>()
            .await
            .change_context(SinkError::Runtime)?;

        assert_eq!(new_docs.len(), 1);
        let new_doc = &new_docs[0];
        assert_eq!(new_doc.get_str("v0").unwrap(), "a");
        assert_eq!(new_doc.get_str("v1").unwrap(), "a");
    }

    Ok(())
}
