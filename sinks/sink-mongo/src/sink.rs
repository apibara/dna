use std::time::Instant;

use apibara_dna_protocol::dna::{common::Cursor, stream::DataFinality};
use apibara_sink_common::batching::Batcher;
use apibara_sink_common::{Context, CursorAction, DisplayCursor, Sink, ValueExt};
use apibara_sink_common::{SinkError, SinkErrorResultExt};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::{FutureExt, TryStreamExt};
use mongodb::bson::{doc, to_document, Bson, Document};
use mongodb::ClientSession;
use std::collections::HashMap;

use mongodb::options::{UpdateModifications, UpdateOptions};
use mongodb::{options::ClientOptions, Client, Collection};

use serde_json::Value;
use tracing::{debug, error, info, warn, Instrument};

use crate::configuration::SinkMongoOptions;

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
    replace_data_inside_transaction: bool,
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
    type Error = SinkError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("connecting to database");
        let connection_string = options
            .connection_string
            .runtime_error("missing connection string")?;

        let db_name = options.database.runtime_error("missing database name")?;

        if options.collection_name.is_some() && options.collection_names.is_some() {
            // TODO: consider using `SinkError::Configuration` instead
            return Err(SinkError::runtime_error(
                "conflicting collection_name and collection_names options",
            ));
        }
        let collection_names = options
            .collection_name
            .map(|c| vec![c])
            .or(options.collection_names)
            .and_then(|vec| (!vec.is_empty()).then_some(vec))
            .runtime_error("missing collection name or collection names")?;

        let client_options = ClientOptions::parse(connection_string)
            .await
            .runtime_error("failed to parse mongo connection string")?;

        let client =
            Client::with_options(client_options).runtime_error("failed to create mongo client")?;

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
            replace_data_inside_transaction: options
                .replace_data_inside_transaction
                .unwrap_or(false),
        })
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        let mut session = self
            .client
            .start_session(None)
            .await
            .runtime_error("failed to create mongo session")?;

        self.handle_data_with_session(&mut session, ctx, batch)
            .await
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_replace(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        let mut session = self
            .client
            .start_session(None)
            .await
            .runtime_error("failed to create mongo session")?;

        if self.replace_data_inside_transaction {
            session
            .with_transaction(
                self,
                |session, sink| {
                    async move {
                        sink.handle_invalidate_with_session(session, &ctx.cursor)
                            .await
                            .map_err(|err| {
                                error!(err = ?err, "failed to invalidate data inside replace transaction");
                                mongodb::error::Error::custom(err)
                            })?;

                        sink.handle_data_with_session(session, ctx, batch)
                            .await
                            .map_err(|err| {
                                error!(err = ?err, "failed to insert data inside replace transaction");
                                mongodb::error::Error::custom(err)
                            })
                    }
                    .boxed()
                },
                None,
            )
            .await
            .runtime_error("failed to replace data inside transaction")
        } else {
            let mut session = self
                .client
                .start_session(None)
                .await
                .runtime_error("failed to create mongo session")?;
            self.handle_invalidate_with_session(&mut session, &ctx.cursor)
                .await?;
            self.handle_data_with_session(&mut session, ctx, batch)
                .await
        }
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        let mut session = self
            .client
            .start_session(None)
            .await
            .runtime_error("failed to create mongo session")?;

        self.handle_invalidate_with_session(&mut session, cursor)
            .await
    }
}

impl MongoSink {
    #[tracing::instrument(skip_all, err(Debug))]
    pub fn collection(&self, collection_name: &str) -> Result<&Collection<Document>, SinkError> {
        self.collections
            .get(collection_name)
            .runtime_error(&format!("collection '{collection_name}' not found"))
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_data_with_session(
        &mut self,
        session: &mut ClientSession,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, SinkError> {
        info!(ctx = %ctx, "handling data");
        let batch = batch
            .as_array_of_objects()
            .unwrap_or(&Vec::<Value>::new())
            .to_vec();

        if ctx.finality != DataFinality::Finalized {
            self.insert_data(session, &ctx.end_cursor, &batch).await?;
            return Ok(CursorAction::Persist);
        }

        match self.batcher.handle_data(ctx, &batch).await {
            Ok((action, None)) => Ok(action),
            Ok((action, Some((end_cursor, batch)))) => {
                self.insert_data(session, &end_cursor, &batch).await?;
                self.batcher.buffer.clear();
                Ok(action)
            }
            Err(e) => Err(e).change_context(SinkError::Runtime),
        }
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_invalidate_with_session(
        &mut self,
        session: &mut ClientSession,
        cursor: &Option<Cursor>,
    ) -> Result<(), SinkError> {
        debug!(cursor = %DisplayCursor(cursor), "handling invalidate");

        if self.batcher.is_batching() && !self.batcher.is_flushed() {
            self.insert_data(
                session,
                &self.batcher.buffer.end_cursor,
                &self.batcher.buffer.data,
            )
            .await?;
            self.batcher.buffer.clear();
        }

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
            let collection_name = collection.name();

            collection
                .delete_many_with_session(delete_query.clone(), None, session)
                .instrument(tracing::info_span!(
                    "invalidate data (delete)",
                    collection = collection_name
                ))
                .await
                .runtime_error("failed to invalidate data (delete)")?;

            collection
                .update_many_with_session(
                    unclamp_query.clone(),
                    unset_cursor_to.clone(),
                    None,
                    session,
                )
                .instrument(tracing::info_span!(
                    "invalidate data (update_many)",
                    collection = collection_name
                ))
                .await
                .runtime_error("failed to invalidate data (update)")?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, err(Debug))]
    pub async fn insert_data(
        &self,
        session: &mut ClientSession,
        end_cursor: &Cursor,
        values: &[Value],
    ) -> Result<(), SinkError> {
        // If there are no values, we don't need to do anything.
        if values.is_empty() {
            return Ok(());
        }

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
            .runtime_error("failed to convert batch to mongo document")?;

        let mut docs_map: HashMap<String, Vec<Document>> = HashMap::new();

        if self.collections.len() > 1 {
            for doc in docs.iter_mut() {
                let collection_name = doc
                    .remove("collection")
                    .runtime_error("document missing collection key")?
                    .as_str()
                    .runtime_error("collection is not a string")?
                    .to_string();

                if let Mode::Standard = self.mode {
                    *doc = doc
                        .get("data")
                        .runtime_error("document missing data key")?
                        .as_document()
                        .runtime_error("data is not a document")?
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

        match &self.mode {
            Mode::Standard => self.insert_logs_data(end_cursor, docs_map, session).await,
            Mode::Entity => {
                self.insert_entities_data(end_cursor, docs_map, session)
                    .await
            }
        }
    }

    #[tracing::instrument(skip_all, err(Debug))]
    pub async fn insert_logs_data(
        &self,
        end_cursor: &Cursor,
        docs_map: HashMap<String, Vec<Document>>,
        session: &mut ClientSession,
    ) -> Result<(), SinkError> {
        let cursor = doc! {
            "from": end_cursor.order_key as i64,
        };

        for (collection_name, mut docs) in docs_map {
            docs.iter_mut().for_each(|doc| doc.add_cursor(&cursor));
            self.collection(&collection_name)?
                .insert_many_with_session(docs, None, session)
                .await
                .runtime_error("failed to insert data (logs)")?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, err(Debug))]
    pub async fn insert_entities_data(
        &self,
        end_cursor: &Cursor,
        docs_map: HashMap<String, Vec<Document>>,
        session: &mut ClientSession,
    ) -> Result<(), SinkError> {
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
                        .runtime_error("document missing entity key")?
                        .as_document()
                        .runtime_error("entity is not an object")?;

                    let update = doc
                        .get("update")
                        .runtime_error("document missing update key")?;

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
                                        .runtime_error(
                                            "update is expected to be a document or pipeline",
                                        )
                                        .map(|stage| stage.clone())
                                })
                                .collect::<Result<Vec<_>, SinkError>>()?;
                            pipeline.push(doc! { "$set": { "_cursor": new_cursor.clone() } });
                            UpdateModifications::Pipeline(pipeline)
                        }
                        _ => {
                            return Err(SinkError::runtime_error(
                                "update is expected to be a document or pipeline",
                            ));
                        }
                    };

                    Ok((entity.clone(), update))
                })
                .collect::<Result<Vec<_>, SinkError>>()?;

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
                .runtime_error("failed to find existing documents")?
                .stream(session)
                .try_collect::<Vec<_>>()
                .await
                .runtime_error("failed to fetch find results")?;

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
                    .runtime_error("failed to insert entities (update existing)")?;

                // duplicate existing rows so that the update operation has something to work on
                existing_docs
                    .iter_mut()
                    .for_each(|doc| doc.replace_cursor(&new_cursor));

                self.collection(&collection_name)?
                    .insert_many_with_session(existing_docs, None, session)
                    .await
                    .runtime_error("failed to insert entities (insert copies)")?;
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
                    .runtime_error("failed to insert entities (update entities)")?;
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
