use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use mongodb::bson::{doc, to_bson, Bson, Document};

use mongodb::{options::ClientOptions, Client, Collection};

use serde_json::Value;
use tracing::{info, warn};

use crate::configuration::SinkMongoOptions;

#[derive(Debug, thiserror::Error)]
pub enum SinkMongoError {
    #[error("Missing connection string")]
    MissingConnectionString,
    #[error("Missing database name")]
    MissingDatabaseName,
    #[error("Missing collection name")]
    MissingCollectionName,
    #[error("Mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("Mongo serialization error: {0}")]
    Serialization(#[from] mongodb::bson::ser::Error),
}

pub struct MongoSink {
    collection: Collection<Document>,
}

#[async_trait]
impl Sink for MongoSink {
    type Options = SinkMongoOptions;
    type Error = SinkMongoError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("mongo: connecting to database");
        let connection_string = options
            .connection_string
            .ok_or(Self::Error::MissingConnectionString)?;
        let db_name = options.database.ok_or(Self::Error::MissingDatabaseName)?;
        let collection_name = options
            .collection_name
            .ok_or(Self::Error::MissingCollectionName)?;

        let client_options = ClientOptions::parse(connection_string).await?;

        let client = Client::with_options(client_options)?;
        let db = client.database(&db_name);
        let collection = db.collection::<Document>(&collection_name);

        Ok(Self { collection })
    }

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error> {
        info!(
            cursor = %DisplayCursor(cursor),
            end_cursor = %end_cursor,
            finality = ?finality,
            "mongo: inserting data"
        );

        let Some(values) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            return Ok(());
        };

        if values.is_empty() {
            return Ok(());
        }

        // convert to u32 because that's the maximum bson can handle
        let block_number = u32::try_from(end_cursor.order_key).unwrap();

        let documents: Vec<Document> = values
            .iter()
            .map(|document| match to_bson(document)? {
                Bson::Document(mut doc) => {
                    doc.insert("_cursor", block_number);
                    Ok(Some(doc))
                }
                value => {
                    warn!(value = ?value, "batch value is not an object");
                    Ok(None)
                }
            })
            .flat_map(|item: Result<Option<Document>, Self::Error>| item.transpose())
            .collect::<Result<Vec<_>, _>>()?;

        self.collection.insert_many(documents, None).await?;

        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "mongo: handling invalidate");

        let query = if let Some(cursor) = cursor {
            // convert to u32 because that's the maximum bson can handle
            let block_number = u32::try_from(cursor.order_key).unwrap();
            doc! { "_cursor": { "$gt": block_number } }
        } else {
            doc! { "_cursor": { "$gte": 0} }
        };

        self.collection.delete_many(query, None).await?;

        Ok(())
    }
}
