use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{CursorAction, DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, to_document, Bson, Document};
use mongodb::ClientSession;

use mongodb::options::{UpdateModifications, UpdateOptions};
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
    #[error("Document key is missing: {0}. You must return an object with all keys specified in the entity keys option.")]
    DocumentMissingKey(String),
    #[error("Document key is not an object: {0}.")]
    ObjectExpected(String),
    #[error("Update operation is not an object or a pipeline: {0}.")]
    ObjectOrPipelineExpected(String),
    #[error("Mongo error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("Mongo serialization error: {0}")]
    Serialization(#[from] mongodb::bson::ser::Error),
}

pub struct MongoSink {
    pub collection: Collection<Document>,
    client: Client,
    mode: Mode,
}

enum Mode {
    /// Store entities as immutable documents.
    Logs,
    /// Store entities as mutable documents (entities).
    Entity,
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

        let entity_mode = options.entity_mode.unwrap_or(false);
        let mode = if entity_mode {
            Mode::Entity
        } else {
            Mode::Logs
        };

        Ok(Self {
            collection,
            client,
            mode,
        })
    }

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        info!(
            cursor = %DisplayCursor(cursor),
            end_cursor = %end_cursor,
            finality = ?finality,
            "mongo: inserting data"
        );

        let Some(values) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            return Ok(CursorAction::Persist);
        };

        if values.is_empty() {
            return Ok(CursorAction::Persist);
        }

        self.insert_data(end_cursor, values).await?;

        Ok(CursorAction::Persist)
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "mongo: handling invalidate");

        let mut session = self.client.start_session(None).await?;

        let (delete_query, unclamp_query) = if let Some(cursor) = cursor {
            // convert to u32 because that's the maximum bson can handle
            let block_number = u32::try_from(cursor.order_key).unwrap();
            let del = doc! { "_cursor.from": { "$gt": block_number } };
            let unclamp = doc! { "_cursor.to": { "$gt": block_number } };
            (del, unclamp)
        } else {
            let del = doc! { "_cursor.from": { "$gte": 0} };
            let unclamp = doc! { "_cursor.to": { "$gte": 0 } };
            (del, unclamp)
        };

        let unset_cursor_to = doc! { "$set": { "_cursor.to": Bson::Null } };

        self.collection
            .delete_many_with_session(delete_query, None, &mut session)
            .await?;
        self.collection
            .update_many_with_session(unclamp_query, unset_cursor_to, None, &mut session)
            .await?;

        Ok(())
    }
}

impl MongoSink {
    pub async fn insert_data(
        &self,
        end_cursor: &Cursor,
        values: &[Value],
    ) -> Result<(), SinkMongoError> {
        let docs = values
            .iter()
            .map(to_document)
            .collect::<Result<Vec<_>, _>>()?;

        let mut session = self.client.start_session(None).await?;

        match &self.mode {
            Mode::Logs => self.insert_logs_data(end_cursor, docs, &mut session).await,
            Mode::Entity => {
                self.insert_entities_data(end_cursor, docs, &mut session)
                    .await
            }
        }
    }

    pub async fn insert_logs_data(
        &self,
        end_cursor: &Cursor,
        mut docs: Vec<Document>,
        session: &mut ClientSession,
    ) -> Result<(), SinkMongoError> {
        let cursor = doc! {
            "from": end_cursor.order_key as i64,
        };

        docs.iter_mut().for_each(|doc| doc.add_cursor(&cursor));

        self.collection
            .insert_many_with_session(docs, None, session)
            .await?;

        Ok(())
    }

    pub async fn insert_entities_data(
        &self,
        end_cursor: &Cursor,
        docs: Vec<Document>,
        session: &mut ClientSession,
    ) -> Result<(), SinkMongoError> {
        let new_cursor = doc! {
            "from": end_cursor.order_key as i64,
        };

        let entities_with_updates = docs
            .into_iter()
            .map(|doc| {
                let entity = doc
                    .get("entity")
                    .ok_or_else(|| SinkMongoError::DocumentMissingKey("entity".to_string()))?
                    .as_document()
                    .ok_or_else(|| SinkMongoError::ObjectExpected("entity".to_string()))?;

                let update = doc
                    .get("update")
                    .ok_or_else(|| SinkMongoError::DocumentMissingKey("update".to_string()))?;

                // Validate that the update is either an object or an array of objects (a
                // pipeline), then
                // - if it's a document, add the cursor to the $set stage (or insert a new $set
                // stage if not present).
                // - add a new stage to the pipeline that sets the cursor.
                let update = match update {
                    Bson::Document(update) => {
                        let mut update = update.clone();
                        match update.get_document_mut("$set") {
                            Err(_) => {
                                update.insert("$set", doc! { "_cursor": new_cursor.clone() });
                            }
                            Ok(set) => {
                                set.insert("_cursor", new_cursor.clone());
                            }
                        }
                        UpdateModifications::Document(update)
                    }
                    Bson::Array(pipeline) => {
                        let mut pipeline = pipeline
                            .iter()
                            .map(|stage| {
                                stage
                                    .as_document()
                                    .ok_or_else(|| {
                                        SinkMongoError::ObjectOrPipelineExpected(
                                            "update".to_string(),
                                        )
                                    })
                                    .map(|stage| stage.clone())
                            })
                            .collect::<Result<Vec<_>, SinkMongoError>>()?;
                        pipeline.push(doc! { "$set": { "_cursor": new_cursor.clone() } });
                        UpdateModifications::Pipeline(pipeline)
                    }
                    _ => {
                        return Err(SinkMongoError::ObjectOrPipelineExpected(
                            "update".to_string(),
                        ));
                    }
                };

                Ok((entity.clone(), update))
            })
            .collect::<Result<Vec<_>, SinkMongoError>>()?;

        let entities_filter = entities_with_updates
            .iter()
            .map(|(entity, _)| entity)
            .collect::<Vec<_>>();

        // get previous entities values, if any
        let existing_docs_query = doc! {
            "$and": [
                doc! { "$or": entities_filter.clone() },
                doc! { "_cursor.to": Bson::Null },
            ]
        };

        let mut existing_docs = self
            .collection
            .find_with_session(Some(existing_docs_query.clone()), None, session)
            .await?
            .stream(session)
            .try_collect::<Vec<_>>()
            .await?;

        if !existing_docs.is_empty() {
            // update validity of previous values
            let clamp_cursor = doc! {
                "$set": {
                    "_cursor.to": end_cursor.order_key as i64,
                }
            };

            self.collection
                .update_many_with_session(existing_docs_query, clamp_cursor, None, session)
                .await?;

            // duplicate existing rows so that the update operation has something to work on
            existing_docs
                .iter_mut()
                .for_each(|doc| doc.replace_cursor(&new_cursor));

            self.collection
                .insert_many_with_session(existing_docs, None, session)
                .await?;
        }

        // update values as specified by user
        let update_options = UpdateOptions::builder().upsert(true).build();

        for (mut doc_filter, update) in entities_with_updates {
            doc_filter.insert("_cursor.to", Bson::Null);
            self.collection
                .update_many_with_session(doc_filter, update, Some(update_options.clone()), session)
                .await?;
        }

        Ok(())
    }
}

trait DocumentExt {
    fn add_cursor(&mut self, cursor: &Document);
    fn replace_cursor(&mut self, cursor: &Document);
}

impl DocumentExt for Document {
    fn add_cursor(&mut self, cursor: &Document) {
        self.insert("_cursor", cursor);
    }

    fn replace_cursor(&mut self, cursor: &Document) {
        self.remove("_id");
        self.insert("_cursor", cursor);
    }
}
