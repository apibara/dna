use std::fmt;
use std::time::Instant;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::batching::Batcher;
use apibara_sink_common::{Context, CursorAction, DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::TryStreamExt;
use mongodb::bson::{doc, to_document, Bson, Document};
use mongodb::ClientSession;
use std::collections::HashMap;

use mongodb::options::{UpdateModifications, UpdateOptions};
use mongodb::{options::ClientOptions, Client, Collection};

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::configuration::SinkMongoOptions;

#[derive(Debug)]
pub struct SinkMongoError;
impl error_stack::Context for SinkMongoError {}

impl fmt::Display for SinkMongoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("mongo sink operation failed")
    }
}

pub struct Batch {
    pub data: Vec<Value>,
    pub end_cursor: Cursor,
    pub start_at: Instant,
}
impl Default for Batch {
    fn default() -> Self {
        Self::new()
    }
}

impl Batch {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            start_at: Instant::now(),
            end_cursor: Cursor::default(),
        }
    }
}

pub struct MongoSink {
    pub collections: HashMap<String, Collection<Document>>,
    invalidate: Option<Document>,
    client: Client,
    mode: Mode,
    pub batcher: Batcher,
}

enum Mode {
    /// Store entities as immutable documents.
    Standard,
    /// Store entities as mutable documents (entities).
    Entity,
}

#[async_trait]
impl Sink for MongoSink {
    type Options = SinkMongoOptions;
    type Error = SinkMongoError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("connecting to database");
        let connection_string = options
            .connection_string
            .ok_or(SinkMongoError)
            .attach_printable("missing connection string")?;
        let db_name = options
            .database
            .ok_or(SinkMongoError)
            .attach_printable("missing database name")?;

        if options.collection_name.is_some() && options.collection_names.is_some() {
            return Err(SinkMongoError)
                .attach_printable("conflicting collection_name and collection_names options");
        }
        let collection_names = options
            .collection_name
            .map(|c| vec![c])
            .or(options.collection_names)
            .and_then(|vec| (!vec.is_empty()).then_some(vec))
            .ok_or(SinkMongoError)
            .attach_printable("missing collection name or collection names")?;

        let client_options = ClientOptions::parse(connection_string)
            .await
            .change_context(SinkMongoError)
            .attach_printable("failed to parse mongo connection string")?;

        let client = Client::with_options(client_options)
            .change_context(SinkMongoError)
            .attach_printable("failed to create mongo client")?;
        let db = client.database(&db_name);
        let collections: HashMap<String, Collection<Document>> = collection_names
            .into_iter()
            .map(|c| (c.clone(), db.collection::<Document>(&c)))
            .collect();

        let entity_mode = options.entity_mode.unwrap_or(false);
        let mode = if entity_mode {
            Mode::Entity
        } else {
            Mode::Standard
        };

        Ok(Self {
            collections,
            client,
            mode,
            invalidate: options.invalidate,
            batcher: Batcher::by_seconds(options.batch_seconds.unwrap_or_default()),
        })
    }

    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        info!(ctx = %ctx, "handling data");
        let batch = batch
            .as_array_of_objects()
            .unwrap_or(&Vec::<Value>::new())
            .to_vec();

        if ctx.finality != DataFinality::DataStatusFinalized {
            self.insert_data(&ctx.end_cursor, &batch).await?;
            return Ok(CursorAction::Persist);
        }

        match self.batcher.handle_data(ctx, &batch).await {
            Ok((action, None)) => Ok(action),
            Ok((action, Some((end_cursor, batch)))) => {
                self.insert_data(&end_cursor, &batch).await?;
                self.batcher.buffer.clear();
                Ok(action)
            }
            Err(e) => Err(e).change_context(SinkMongoError),
        }
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        debug!(cursor = %DisplayCursor(cursor), "handling invalidate");

        if self.batcher.is_batching() && !self.batcher.is_flushed() {
            self.insert_data(&self.batcher.buffer.end_cursor, &self.batcher.buffer.data)
                .await?;
            self.batcher.buffer.clear();
        }

        let mut session = self
            .client
            .start_session(None)
            .await
            .change_context(SinkMongoError)
            .attach_printable("failed to create mongo session")?;

        let (mut delete_query, mut unclamp_query) = if let Some(cursor) = cursor {
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

        if let Some(invalidate) = &self.invalidate {
            delete_query.extend(invalidate.clone());
            unclamp_query.extend(invalidate.clone());
        }

        for collection in self.collections.values() {
            collection
                .delete_many_with_session(delete_query.clone(), None, &mut session)
                .await
                .change_context(SinkMongoError)
                .attach_printable("failed to invalidate data (delete)")?;
            collection
                .update_many_with_session(
                    unclamp_query.clone(),
                    unset_cursor_to.clone(),
                    None,
                    &mut session,
                )
                .await
                .change_context(SinkMongoError)
                .attach_printable("failed to invalidate data (update)")?;
        }

        Ok(())
    }
}

impl MongoSink {
    pub fn collection(
        &self,
        collection_name: &str,
    ) -> Result<&Collection<Document>, SinkMongoError> {
        self.collections
            .get(collection_name)
            .ok_or(SinkMongoError)
            .attach_printable(format!("collection '{collection_name}' not found"))
    }

    pub async fn insert_data(
        &self,
        end_cursor: &Cursor,
        values: &[Value],
    ) -> Result<(), SinkMongoError> {
        // TODO: sometimes, the function fails because it tries to insert empty data
        // I noticed this especially when persistence is enabled and the sink is restarted
        // See the error below:
        // 2024-01-29T14:18:19.602299Z  WARN failed to handle data err=mongo sink operation failed
        // ├╴at sinks/sink-mongo/src/sink.rs:313:18
        // ├╴failed to insert data (logs)
        // │
        // ╰─▶ Kind: An invalid argument was provided: No documents provided to insert_many, labels: {}
        //     ╰╴at sinks/sink-mongo/src/sink.rs:313:18

        if self.collections.len() > 1 {
            let missing_collection_key =
                values.iter().any(|value| value.get("collection").is_none());

            if missing_collection_key {
                warn!("some documents missing 'collection' key while in multi collection mode");
                return Ok(());
            }
        }

        let mut docs = values
            .iter()
            .map(to_document)
            .collect::<std::result::Result<Vec<_>, _>>()
            .change_context(SinkMongoError)
            .attach_printable("failed to convert batch to mongo document")?;

        let mut docs_map: HashMap<String, Vec<Document>> = HashMap::new();

        if self.collections.len() > 1 {
            for doc in docs.iter_mut() {
                let collection_name = doc
                    .remove("collection")
                    .ok_or(SinkMongoError)
                    .attach_printable("document missing collection key")?
                    .as_str()
                    .ok_or(SinkMongoError)
                    .attach_printable("collection is not a string")?
                    .to_string();

                if let Mode::Standard = self.mode {
                    *doc = doc
                        .get("data")
                        .ok_or(SinkMongoError)
                        .attach_printable("document missing data key")?
                        .as_document()
                        .ok_or(SinkMongoError)
                        .attach_printable("data is not a document")?
                        .clone();
                }

                if !docs_map.contains_key(&collection_name) {
                    docs_map.insert(collection_name.clone(), Vec::new());
                }

                // Safe unwrap because of the above if statement where we add the
                // `collection_name` key if it doesn't exist
                docs_map
                    .entry(collection_name)
                    .or_default()
                    .push(doc.clone())
            }
        } else {
            // Safe unwrap because we already made sure we have at least one collection
            let collection_name = self.collections.values().next().unwrap().name().to_string();
            docs_map.insert(collection_name, docs);
        }

        let mut session = self
            .client
            .start_session(None)
            .await
            .change_context(SinkMongoError)
            .attach_printable("failed to create mongo session")?;

        match &self.mode {
            Mode::Standard => {
                self.insert_logs_data(end_cursor, docs_map, &mut session)
                    .await
            }
            Mode::Entity => {
                self.insert_entities_data(end_cursor, docs_map, &mut session)
                    .await
            }
        }
    }

    pub async fn insert_logs_data(
        &self,
        end_cursor: &Cursor,
        docs_map: HashMap<String, Vec<Document>>,
        session: &mut ClientSession,
    ) -> Result<(), SinkMongoError> {
        let cursor = doc! {
            "from": end_cursor.order_key as i64,
        };

        for (collection_name, mut docs) in docs_map {
            docs.iter_mut().for_each(|doc| doc.add_cursor(&cursor));
            self.collection(&collection_name)?
                .insert_many_with_session(docs, None, session)
                .await
                .change_context(SinkMongoError)
                .attach_printable("failed to insert data (logs)")?;
        }

        Ok(())
    }

    pub async fn insert_entities_data(
        &self,
        end_cursor: &Cursor,
        docs_map: HashMap<String, Vec<Document>>,
        session: &mut ClientSession,
    ) -> Result<(), SinkMongoError> {
        let new_cursor = doc! {
            "from": end_cursor.order_key as i64,
        };

        // TODO: use transactions for performance
        // See https://www.mongodb.com/docs/upcoming/core/transactions/

        for (collection_name, docs) in docs_map {
            let entities_with_updates = docs
                .into_iter()
                .map(|doc| {
                    let entity = doc
                        .get("entity")
                        .ok_or(SinkMongoError)
                        .attach_printable("document missing entity key")?
                        .as_document()
                        .ok_or(SinkMongoError)
                        .attach_printable("entity is not an object")?;

                    let update = doc
                        .get("update")
                        .ok_or(SinkMongoError)
                        .attach_printable("document missing update key")?;

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
                                        .ok_or(SinkMongoError)
                                        .attach_printable(
                                            "update is expected to be a document or pipeline",
                                        )
                                        .map(|stage| stage.clone())
                                })
                                .collect::<Result<Vec<_>, SinkMongoError>>()?;
                            pipeline.push(doc! { "$set": { "_cursor": new_cursor.clone() } });
                            UpdateModifications::Pipeline(pipeline)
                        }
                        _ => {
                            return Err(SinkMongoError).attach_printable(
                                "update is expected to be a document or pipeline",
                            );
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
                .collection(&collection_name)?
                .find_with_session(Some(existing_docs_query.clone()), None, session)
                .await
                .change_context(SinkMongoError)
                .attach_printable("failed to find existing documents")?
                .stream(session)
                .try_collect::<Vec<_>>()
                .await
                .change_context(SinkMongoError)
                .attach_printable("failed to fetch find results")?;

            if !existing_docs.is_empty() {
                // update validity of previous values
                let clamp_cursor = doc! {
                    "$set": {
                        "_cursor.to": end_cursor.order_key as i64,
                    }
                };

                self.collection(&collection_name)?
                    .update_many_with_session(existing_docs_query, clamp_cursor, None, session)
                    .await
                    .change_context(SinkMongoError)
                    .attach_printable("failed to insert entities (update existing)")?;

                // duplicate existing rows so that the update operation has something to work on
                existing_docs
                    .iter_mut()
                    .for_each(|doc| doc.replace_cursor(&new_cursor));

                self.collection(&collection_name)?
                    .insert_many_with_session(existing_docs, None, session)
                    .await
                    .change_context(SinkMongoError)
                    .attach_printable("failed to insert entities (insert copies)")?;
            }

            // update values as specified by user
            let update_options = UpdateOptions::builder().upsert(true).build();

            for (mut doc_filter, update) in entities_with_updates {
                doc_filter.insert("_cursor.to", Bson::Null);
                self.collection(&collection_name)?
                    .update_many_with_session(
                        doc_filter,
                        update,
                        Some(update_options.clone()),
                        session,
                    )
                    .await
                    .change_context(SinkMongoError)
                    .attach_printable("failed to insert entities (update entities)")?;
            }
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
