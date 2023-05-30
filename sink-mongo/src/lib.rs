use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::{error::Error, options::ClientOptions, Client, Collection};

use serde_json::{json, Value};
use tracing::{info, warn};

pub struct MongoSink {
    collection: Collection<Value>,
}

impl MongoSink {
    pub async fn new(
        mongo_url: String,
        db_name: String,
        collection_name: String,
    ) -> Result<Self, Error> {
        // Parse a connection string into an options struct.
        let client_options = ClientOptions::parse(mongo_url).await?;

        let client = Client::with_options(client_options)?;
        let db = client.database(&db_name);
        let collection = db.collection::<Value>(&collection_name);

        Ok(Self { collection })
    }
}

#[async_trait]
impl Sink for MongoSink {
    type Error = Error;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error> {
        info!(
            cursor = ?cursor,
            end_cursor = ?end_cursor,
            finality = ?finality,
            batch = ?batch,
            "inserting data to mongo"
        );
        match batch {
            Value::Array(array) => {
                let documents: Vec<Value> = array
                    .iter()
                    .map(|element| json!({"data": element, "_cursor": end_cursor.order_key.to_string()}))
                    .collect();
                self.collection.insert_many(documents, None).await?;
            }
            value => {
                warn!(value = ?value, "batch value is not an array");
                let document = json!({"data": value, "_cursor": end_cursor.order_key.to_string()});
                self.collection.insert_one(document, None).await?;
            }
        }
        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = ?cursor, "invalidating data from mongo");
        if let Some(cursor) = cursor {
            let query = doc! {
                "_cursor": {
                    "$gte": cursor.order_key.to_string(),
                },
            };
            self.collection.delete_many(query, None).await?;
        }
        Ok(())
    }
}
