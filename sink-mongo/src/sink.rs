use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use mongodb::bson::{doc, to_document, Document};
use mongodb::{options::ClientOptions, Client, Collection};
use serde::Serialize;
use serde_json::Value;
use tracing::{info, warn};
use crate::configuration::SinkMongoOptions;
use mockall::automock;

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

#[automock]
#[async_trait]
pub trait MongoCollection<T: Serialize + Send + Sync> {
    async fn insert_many(
        &self,
        documents: Vec<T>,
        options: Option<mongodb::options::InsertManyOptions>,
    ) -> mongodb::error::Result<u64>;

    async fn delete_many(
        &self,
        filter: Document,
        options: Option<mongodb::options::DeleteOptions>,
    ) -> mongodb::error::Result<u64>;
}

pub struct MongoCollectionWrapper<T: Serialize + Send + Sync> {
    collection: Collection<T>,
}

#[async_trait]
impl<T: Serialize + Send + Sync> MongoCollection<T> for MongoCollectionWrapper<T> {
    async fn insert_many(
        &self,
        documents: Vec<T>,
        options: Option<mongodb::options::InsertManyOptions>,
    ) -> mongodb::error::Result<u64> {
        let result = self.collection.insert_many(documents, options).await?;
        Ok(result.inserted_ids.len() as u64)
    }

    async fn delete_many(
        &self,
        filter: Document,
        options: Option<mongodb::options::DeleteOptions>,
    ) -> mongodb::error::Result<u64> {
        let result = self.collection.delete_many(filter, options).await?;
        Ok(result.deleted_count)
    }
}

pub struct MongoSink {
    collection: Box<dyn MongoCollection<Document> + Send + Sync>,
}

impl MongoSink {
    pub fn new(collection: Box<dyn MongoCollection<Document> + Send + Sync>) -> Self {
        Self { collection }
    }
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

        let collection = Box::new(MongoCollectionWrapper { collection });

        Ok(Self::new(collection))
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

        let documents: Vec<Document> = values
            .clone()
            .iter_mut()
            .map(|obj| match obj.as_object_mut() {
                Some(obj) => {
                    obj.insert("_cursor".into(), end_cursor.order_key.into());
                    let doc = to_document(obj).unwrap();
                    Ok(Some(doc))
                }
                None => {
                    warn!("batch value is not an object");
                    Ok(None)
                }
            })
            .flat_map(|item: Result<Option<Document>, Self::Error>| item.transpose())
            .collect::<Result<Vec<_>, _>>()?;

        // the compiler is not happy if we don't set the future to a variable
        let future = self.collection.insert_many(documents, None);
        future.await?;

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

        // the compiler is not happy if we don't set the future to a variable
        let future = self.collection.delete_many(query, None);
        future.await?;

        Ok(())
    }
}
