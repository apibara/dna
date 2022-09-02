//! Ingest blocks from the node.

use std::{fmt::Display, sync::Arc};

use apibara_node::{
    chain_tracker::{ChainTracker, ChainTrackerError},
    db::libmdbx::{Environment, EnvironmentKind},
};
use starknet::{
    core::types::BlockId,
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio::sync::broadcast::{error::SendError, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{
    block_builder::{BlockBuilder, BlockBuilderError},
    core::Block,
};

pub struct BlockIngestor<E: EnvironmentKind> {
    chain: ChainTracker<Block, E>,
    block_builder: BlockBuilder,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestorError {
    #[error("error tracking chain state")]
    ChainTracker(#[from] ChainTrackerError),
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
        let chain = ChainTracker::new(db)?;
        let block_builder = BlockBuilder::new(client);
        Ok(BlockIngestor {
            chain,
            block_builder,
        })
    }

    pub async fn start(&self, block_tx: Sender<Block>, ct: CancellationToken) -> Result<()> {
        let current_head = self.block_builder.latest_block().await?;
        self.chain.update_head(&current_head)?;
        info!(head = %current_head.block_hash.unwrap_or_default(), "updated head");

        let mut starting_block_number = 0;
        if let Some(latest_block) = self.chain.latest_indexed_block()? {
            info!("check reorg while offline");

            let block = self
                .block_builder
                .block_by_number_with_backoff(latest_block.block_number, ct.clone())
                .await?;

            if block.block_hash != latest_block.block_hash {
                error!("reorg while offline");
                todo!()
            }
            starting_block_number = latest_block.block_number + 1;
        }

        info!(block_number = %starting_block_number, "starting block ingestion");

        let mut current_block_number = starting_block_number;

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

    #[tracing::instrument(skip(self, block_tx, ct))]
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

        let _state_change = self.chain.update_indexed_block(block)?;
        // broadcast new block
        // block_tx.send(block)?;

        Ok(())
    }
}
