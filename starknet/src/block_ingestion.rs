//! Ingest blocks from the node.

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use starknet::{
    core::types::BlockId,
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::state::{StateStorage, StateStorageError};

pub struct BlockIngestor<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    client: Arc<SequencerGatewayProvider>,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestorError {
    #[error("error with state storage")]
    StateStorage(#[from] StateStorageError),
    #[error("error fetching data from sequencer gateway")]
    Gateway(#[from] SequencerGatewayProviderError),
    #[error("unexpected pending block")]
    UnexpectedPendingBlock { block_number: u64 },
}

pub type Result<T> = std::result::Result<T, BlockIngestorError>;

impl<E> BlockIngestor<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, client: Arc<SequencerGatewayProvider>) -> Self {
        BlockIngestor { db, client }
    }

    pub async fn start(&self, ct: CancellationToken) -> Result<()> {
        let state_storage = StateStorage::new(self.db.clone())?;

        let starting_block = state_storage
            .get_state()?
            .map(|s| s.block_number + 1)
            .unwrap_or(0);
        // TODO: Check latest indexed block.
        // - compare stored hash with live hash to detect reorgs
        //   while node was not running.

        info!(starting_block = ?starting_block, "start block ingestion");

        let mut current_block_number = starting_block;

        loop {
            if ct.is_cancelled() {
                break;
            }

            // fetch block
            let block = self
                .client
                .get_block(BlockId::Number(current_block_number))
                .await?;

            let block_number =
                block
                    .block_number
                    .ok_or_else(|| BlockIngestorError::UnexpectedPendingBlock {
                        block_number: current_block_number,
                    })?;
            let block_hash =
                block
                    .block_hash
                    .ok_or_else(|| BlockIngestorError::UnexpectedPendingBlock {
                        block_number: current_block_number,
                    })?;

            info!(block_number = ?block_number, "got block");
            current_block_number += 1;

            // update state
            state_storage.update_state(block_number, block_hash)?;
        }

        Ok(())
    }
}
