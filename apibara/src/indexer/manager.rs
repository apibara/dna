use std::{pin::Pin, sync::Arc};

use anyhow::{Error, Result};
use futures::{Stream, StreamExt};

use crate::{
    chain::{ChainProvider, EventFilter},
    persistence::Id,
};

use super::{start_indexer, ClientToIndexerMessage, IndexerPersistence, Message, State};

const DEFAULT_BLOCK_BATCH_SIZE: usize = 200;

#[derive(Debug)]
pub struct IndexerManager<IP: IndexerPersistence> {
    persistence: Arc<IP>,
}

impl<IP: IndexerPersistence> IndexerManager<IP> {
    pub fn new(persistence: Arc<IP>) -> IndexerManager<IP> {
        Self { persistence }
    }

    pub async fn create_indexer(
        &self,
        id: &Id,
        filters: Vec<EventFilter>,
        index_from_block: u64,
    ) -> Result<State> {
        let existing = self.persistence.get_indexer(id).await?;

        // TODO: should return already exists error type
        if existing.is_some() {
            return Err(Error::msg("already exists"));
        }

        let state = State {
            id: id.clone(),
            index_from_block,
            filters,
            block_batch_size: DEFAULT_BLOCK_BATCH_SIZE,
            indexed_to_block: None,
        };

        self.persistence.create_indexer(&state).await?;

        Ok(state)
    }

    pub async fn get_indexer(&self, id: &Id) -> Result<Option<State>> {
        self.persistence.get_indexer(id).await
    }

    pub async fn list_indexer(&self) -> Result<Vec<State>> {
        self.persistence.list_indexer().await
    }

    pub async fn delete_indexer(&self, id: &Id) -> Result<State> {
        let existing = self.persistence.get_indexer(id).await?;

        // TODO: should return not found error type
        match existing {
            None => Err(Error::msg("not found")),
            Some(existing) => {
                self.persistence.delete_indexer(id).await?;
                Ok(existing)
            }
        }
    }

    pub async fn connect_indexer<S, P>(
        &self,
        mut client_stream: Pin<Box<S>>,
        provider: Arc<P>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Message>> + Send>>>
    where
        S: Stream<Item = Result<ClientToIndexerMessage>> + Send + 'static,
        P: ChainProvider,
    {
        // first message MUST be a connect message
        // when we receive that message, use the provided indexer id
        // value to lookup the indexer state and resume indexing from there.
        let message = client_stream
            .next()
            .await
            .ok_or_else(|| Error::msg("client did not send connect message"))??;
        let indexer_state = if let ClientToIndexerMessage::Connect(indexer_id) = message {
            self.persistence
                .get_indexer(&indexer_id)
                .await?
                .ok_or_else(|| Error::msg("indexer not found"))?
        } else {
            return Err(Error::msg("must send Connect message first"));
        };

        // TODO: track all indexers' handles and report their error
        let (_indexer_handle, indexer_stream) = start_indexer(
            &indexer_state,
            client_stream,
            provider,
            self.persistence.clone(),
        )
        .await?;

        Ok(Box::pin(indexer_stream))
    }
}
