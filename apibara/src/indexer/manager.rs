use std::{pin::Pin, sync::Arc};

use anyhow::{Error, Result};
use futures::{Stream, StreamExt};
use tracing::error;

use crate::{
    chain::EventFilter,
    network_manager::NetworkManager,
    persistence::{Id, NetworkName},
};

use super::{start_indexer, ClientToIndexerMessage, IndexerPersistence, Message, State};

const DEFAULT_BLOCK_BATCH_SIZE: usize = 200;

pub struct IndexerManager<IP: IndexerPersistence> {
    network_manager: Arc<NetworkManager>,
    persistence: Arc<IP>,
}

impl<IP: IndexerPersistence> IndexerManager<IP> {
    pub fn new(network_manager: Arc<NetworkManager>, persistence: Arc<IP>) -> IndexerManager<IP> {
        Self {
            network_manager,
            persistence,
        }
    }

    pub async fn create_indexer(
        &self,
        id: &Id,
        network_name: NetworkName,
        filters: Vec<EventFilter>,
        index_from_block: u64,
    ) -> Result<State> {
        let existing = self.persistence.get_indexer(id).await?;

        // TODO: should return already exists error type
        if existing.is_some() {
            return Err(Error::msg("already exists"));
        }

        // Check if network exists.
        let network = self
            .network_manager
            .find_network_by_name(&network_name)
            .ok_or_else(|| Error::msg(format!("network not found: {}", network_name)))?;

        let state = State {
            id: id.clone(),
            network,
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

    pub async fn connect_indexer<S>(
        &self,
        mut client_stream: Pin<Box<S>>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Message>> + Send>>>
    where
        S: Stream<Item = Result<ClientToIndexerMessage>> + Send + 'static,
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

        let provider = self
            .network_manager
            .provider_for_network(indexer_state.network.name())
            .await?;

        let (indexer_handle, indexer_stream) = start_indexer(
            &indexer_state,
            client_stream,
            provider,
            self.persistence.clone(),
        )
        .await?;

        tokio::spawn(async move {
            if let Ok(Err(err)) = indexer_handle.await {
                error!("indexer service error: {:?}", err);
            }
        });

        Ok(Box::pin(indexer_stream))
    }
}
