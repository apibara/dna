use mongodb::{
    bson::{doc, Document},
    options::FindOptions,
    Collection,
};
use testcontainers::{core::WaitFor, GenericImage};

use apibara_core::node::v1alpha2::Cursor;
use futures_util::TryStreamExt;
use mongodb::bson::to_document;
use serde_json::{json, Value};

pub fn new_mongo_image() -> GenericImage {
    GenericImage::new("mongo", "7.0.1")
        .with_wait_for(WaitFor::message_on_stdout("Waiting for connections"))
}

pub fn new_cursor(order_key: u64) -> Cursor {
    Cursor {
        order_key,
        unique_key: order_key.to_be_bytes().to_vec(),
    }
}

pub async fn get_all_docs(collection: &Collection<Document>) -> Vec<Document> {
    let find_options = Some(
        FindOptions::builder()
            .projection(Some(doc! {"_id": 0}))
            .build(),
    );

    collection
        .find(None, find_options)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

pub fn new_not_array_of_objects() -> Value {
    json!([0, { "key": "value" }, 1])
}

pub fn new_docs(start_cursor: &Option<Cursor>, end_cursor: &Cursor) -> Vec<Document> {
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
                "_cursor": json!({"from": end_block_num}),
            }))
            .unwrap(),
        );
    }
    batch
}
