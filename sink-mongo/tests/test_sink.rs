use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use apibara_sink_mongo::{MockMongoCollection, MongoSink};
use mockall::predicate;
use mongodb::bson::{doc, to_document, Document};
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

fn new_docs(size: usize, block_number: u64) -> Vec<Document> {
    let mut batch = Vec::new();
    for i in 0..size {
        batch.push(
            // we have to convert first to a json then to a mongo document for
            // the numbers to be handled as u64, doc! macro don't handle u64
            // for some reason
            to_document(&json!({
                "batchNum": i,
                "batchStr": format!("batch_{}", i),
                "_cursor": block_number,
            }))
            .unwrap(),
        );
    }
    batch
}

fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

#[tokio::test]
async fn test_handle_data() -> Result<(), apibara_sink_mongo::SinkMongoError> {
    let mut mock = MockMongoCollection::<Document>::new();

    for i in 0..5 {
        let end_cursor = new_cursor(i + 1);
        let docs = new_docs(1, end_cursor.order_key);
        mock.expect_insert_many()
            .with(predicate::eq(docs), predicate::always())
            .times(1)
            .returning(|docs, _| Ok(docs.len() as u64));
    }

    let mut sink = MongoSink::new(Box::new(mock));

    for i in 0..5 {
        let cursor = Some(new_cursor(i));
        let end_cursor = new_cursor(i + 1);
        let finality = DataFinality::DataStatusFinalized;
        let batch = new_batch(1);

        sink.handle_data(&cursor, &end_cursor, &finality, &batch)
            .await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_handle_invalidate() -> Result<(), apibara_sink_mongo::SinkMongoError> {
    let mut mock = MockMongoCollection::<Document>::new();

    for i in 0..5 {
        let cursor = Some(new_cursor(i));

        let query = doc! {"_cursor": {"$gt": cursor.map(|c| c.order_key as u32).unwrap_or(0)}};

        mock.expect_delete_many()
            .with(predicate::eq(query), predicate::always())
            .times(1)
            .return_const(Ok(1));
    }

    let mut sink = MongoSink::new(Box::new(mock));

    for i in 0..5 {
        let cursor = Some(new_cursor(i));
        sink.handle_invalidate(&cursor).await?;
    }

    Ok(())
}
