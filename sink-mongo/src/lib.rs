use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;
use mongodb::bson::{doc, to_bson, Document};

use mongodb::{error::Error, options::ClientOptions, Client, Collection};

use serde_json::Value;
use tracing::{info, warn};

pub struct MongoSink {
    collection: Collection<Document>,
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
        let collection = db.collection::<Document>(&collection_name);

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
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(
            cursor = %cursor_str,
            end_cursor = %end_cursor,
            finality = ?finality,
            "mongo: inserting data"
        );

        // convert to u32 because that's the maximum bson can handle
        let block_number = u32::try_from(end_cursor.order_key).unwrap();

        match batch {
            Value::Array(array) => {
                // don't store anything since the driver doesn't support empty arrays
                if array.is_empty() {
                    return Ok(());
                }

                let documents: Vec<Document> = array
                    .iter()
                    .map(
                        // Use unwrap because if the data couldn't be converted to bson
                        // we can't write it to Mongo and thus there is point to continue
                        |element| doc! {"data": to_bson(element).unwrap(), "_cursor": block_number},
                    )
                    .collect();
                self.collection.insert_many(documents, None).await?;
            }
            value => {
                warn!(value = ?value, "batch value is not an array");
            }
        }
        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(cursor = %cursor_str, "mongo: deleting data");

        let query = if let Some(cursor) = cursor {
            // convert to u32 because that's the maximum bson can handle
            let block_number = u32::try_from(cursor.order_key).unwrap();
            doc! { "_cursor": { "$gt": block_number } }
        } else {
            doc! {}
        };

        self.collection.delete_many(query, None).await?;

        Ok(())
    }
}
