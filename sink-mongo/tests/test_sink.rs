use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{CursorAction, Sink};
use apibara_sink_mongo::{MongoSink, SinkMongoError, SinkMongoOptions};
use futures_util::StreamExt;
use mongodb::{
    bson::{doc, to_document, Document},
    options::FindOptions,
    Collection,
};
use serde_json::{json, Value};
use testcontainers::{clients, images::mongo::Mongo};

fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

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

fn new_not_array_of_objects() -> Value {
    json!([0, { "key": "value" }, 1])
}

fn new_docs(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> Vec<Document> {
    let mut batch = Vec::new();

    let start_block_num = match start_cursor {
        Some(cursor) => cursor.order_key,
        None => 0,
    };

    let end_block_num = end_cursor.order_key;

    for i in start_block_num..end_block_num {
        batch.push(
            // we have to convert first to a json then to a mongo document for
            // the numbers to be handled as u64, doc! macro don't handle u64
            // for some reason
            to_document(&json!({
                "block_num": i,
                "block_str": format!("block_{}", i),
                "_cursor": end_block_num,
            }))
            .unwrap(),
        );
    }
    batch
}

async fn get_all_docs(collection: &Collection<Document>) -> Vec<Document> {
    let find_options = Some(
        FindOptions::builder()
            .projection(Some(doc! {"_id": 0}))
            .build(),
    );

    collection
        .find(None, find_options)
        .await
        .unwrap()
        .map(|doc| doc.unwrap())
        .collect()
        .await
}

#[tokio::test]
#[ignore]
async fn test_handle_data() -> Result<(), SinkMongoError> {
    let docker = clients::Cli::default();
    let mongo = docker.run(Mongo::default());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
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

        let action = sink
            .handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        assert_eq!(action, CursorAction::Persist);

        all_docs.extend(new_docs(&cursor, &end_cursor));

        let action = sink
            .handle_data(&cursor, &end_cursor, &finality, &new_not_array_of_objects())
            .await?;

        assert_eq!(action, CursorAction::Persist);

        let action = sink
            .handle_data(&cursor, &end_cursor, &finality, &json!([]))
            .await?;

        assert_eq!(action, CursorAction::Persist);
    }

    assert_eq!(all_docs, get_all_docs(&sink.collection).await);

    Ok(())
}

async fn test_handle_invalidate_all(
    invalidate_from: &Option<Cursor>,
) -> Result<(), SinkMongoError> {
    assert!(invalidate_from.is_none() || invalidate_from.clone().unwrap().order_key == 0);

    let docker = clients::Cli::default();
    let mongo = docker.run(Mongo::default());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
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

        let action = sink
            .handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        assert_eq!(action, CursorAction::Persist);

        all_docs.extend(new_docs(&cursor, &end_cursor));
    }

    assert_eq!(all_docs, get_all_docs(&sink.collection).await);

    sink.handle_invalidate(invalidate_from).await?;
    assert_eq!(Vec::<Document>::new(), get_all_docs(&sink.collection).await);

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
    let mongo = docker.run(Mongo::default());
    let port = mongo.get_host_port_ipv4(27017);

    let options = SinkMongoOptions {
        connection_string: Some(format!("mongodb://localhost:{}", port)),
        database: Some("test".into()),
        collection_name: Some("test".into()),
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

        let action = sink
            .handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;

        all_docs.extend(new_docs(&cursor, &end_cursor));

        assert_eq!(action, CursorAction::Persist);
    }

    assert_eq!(all_docs, get_all_docs(&sink.collection).await);

    let invalidate_from = 2;

    sink.handle_invalidate(&Some(new_cursor(invalidate_from)))
        .await?;

    let expected_docs: Vec<Document> = all_docs
        .into_iter()
        .filter(|doc| doc.get_i64("_cursor").unwrap() as u64 <= invalidate_from)
        .collect();

    assert_eq!(expected_docs, get_all_docs(&sink.collection).await);

    Ok(())
}
