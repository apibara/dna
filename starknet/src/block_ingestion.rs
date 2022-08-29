//! Ingest blocks from the node.

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use starknet::{
    core::types::BlockId,
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio::sync::broadcast::{error::SendError, Sender};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_builder::{BlockBuilder, BlockBuilderError},
    core::Block,
    state::{StateStorage, StateStorageError},
};

pub struct BlockIngestor<E: EnvironmentKind> {
    state_storage: StateStorage<E>,
    block_builder: BlockBuilder,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestorError {
    #[error("error with state storage")]
    StateStorage(#[from] StateStorageError),
    #[error("error fetching or parsing block")]
    BlockBuilder(#[from] BlockBuilderError),
    #[error("error broadcasting block")]
    Broadcast(#[from] SendError<Block>),
}

pub type Result<T> = std::result::Result<T, BlockIngestorError>;

impl<E> BlockIngestor<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, client: Arc<SequencerGatewayProvider>) -> Result<Self> {
        let state_storage = StateStorage::new(db)?;
        let block_builder = BlockBuilder::new(client);
        Ok(BlockIngestor {
            state_storage,
            block_builder,
        })
    }

    pub async fn start(&self, block_tx: Sender<Block>, ct: CancellationToken) -> Result<()> {
        let starting_block = self
            .state_storage
            .get_state()?
            .map(|s| s.block_number + 1)
            .unwrap_or(0);
        // TODO: Check latest indexed block.
        // - compare stored hash with live hash to detect reorgs while node was not running.

        info!(starting_block = ?starting_block, "start block ingestion");

        let mut current_block_number = starting_block;

        loop {
            if ct.is_cancelled() {
                break;
            }
            self.fetch_and_broadcast_block(&block_tx, current_block_number, &ct)
                .await?;
            current_block_number += 1;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_and_broadcast_block(
        &self,
        block_tx: &Sender<Block>,
        block_number: u64,
        ct: &CancellationToken,
    ) -> Result<()> {
        // fetch block
        let block = tokio::select! {
            block = self.block_builder.block_by_number_with_backoff(block_number, ct.clone()) => {
                block?
            }
            _ = ct.cancelled() => {
                return Ok(())
            }
        };

        info!(block_number = ?block.block_number, "got block");

        let block_number = block.block_number;
        let block_hash = block.block_hash.clone();

        // broadcast new block
        block_tx.send(block)?;

        // update state
        self.state_storage.update_state(block_number, block_hash)?;

        Ok(())
    }
}
