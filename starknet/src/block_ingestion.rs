//! Ingest blocks from the node.

use std::{collections::VecDeque, sync::Arc, task::Poll};

use apibara_core::pb;
use apibara_node::{
    chain_tracker::{ChainChange, ChainTracker, ChainTrackerError},
    db::libmdbx::EnvironmentKind,
};
use futures::Stream;
use prost::Message;
use starknet::providers::SequencerGatewayProvider;
use tokio::sync::broadcast::{self, error::SendError};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{error, info};

use crate::{
    block_builder::{BlockBuilder, BlockBuilderError},
    core::Block,
};

pub struct BlockIngestor<E: EnvironmentKind> {
    chain: Arc<ChainTracker<Block, E>>,
    block_builder: BlockBuilder,
    block_tx: broadcast::Sender<pb::ConnectResponse>,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestorError {
    #[error("error tracking chain state")]
    ChainTracker(#[from] ChainTrackerError),
    #[error("error fetching or parsing block")]
    BlockBuilder(#[from] BlockBuilderError),
    #[error("error broadcasting block")]
    Broadcast(#[from] SendError<Block>),
    #[error("chain not started syncing")]
    EmptyChain,
}

pub type Result<T> = std::result::Result<T, BlockIngestorError>;

const MESSAGE_CHANNEL_SIZE: usize = 128;

pub struct BackfilledBlockStream<E: EnvironmentKind> {
    chain: Arc<ChainTracker<Block, E>>,
    block: u64,
    indexed: u64,
    rx: broadcast::Receiver<pb::ConnectResponse>,
    buffer: VecDeque<pb::ConnectResponse>,
}

impl<E> BlockIngestor<E>
where
    E: EnvironmentKind,
{
    pub fn new(
        chain: Arc<ChainTracker<Block, E>>,
        client: Arc<SequencerGatewayProvider>,
    ) -> Result<Self> {
        let block_builder = BlockBuilder::new(client);
        let (block_tx, _) = broadcast::channel(MESSAGE_CHANNEL_SIZE);

        Ok(BlockIngestor {
            chain,
            block_builder,
            block_tx,
        })
    }

    /// Subscribe to new chain blocks and reorganizations.
    pub fn subscribe(&self) -> broadcast::Receiver<pb::ConnectResponse> {
        self.block_tx.subscribe()
    }

    pub fn stream_from_sequence(&self, starting_sequence: u64) -> Result<BackfilledBlockStream<E>> {
        let indexed = self
            .chain
            .latest_indexed_block()?
            .ok_or(BlockIngestorError::EmptyChain)?
            .block_number;
        let rx = self.subscribe();

        info!(start = %starting_sequence, "start stream");

        // validate sequence
        Ok(BackfilledBlockStream {
            rx,
            chain: self.chain.clone(),
            block: starting_sequence,
            indexed,
            buffer: VecDeque::default(),
        })
    }

    pub async fn start(&self, ct: CancellationToken) -> Result<()> {
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

            self.fetch_and_broadcast_block(current_block_number, &ct)
                .await?;

            current_block_number += 1;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, ct))]
    async fn fetch_and_broadcast_block(
        &self,
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

        match self.chain.update_indexed_block(block)? {
            ChainChange::Advance(blocks) => {
                info!("chain advanced by {} blocks", blocks.len());
            }
            ChainChange::Reorg(blocks) => {
                info!("chain reorged by {} blocks", blocks.len());
                todo!()
            }
            ChainChange::MissingBlock(block_number, block_hash) => {
                info!("block is missing: {}/{}", block_number, block_hash);
                todo!()
            }
        }
        // broadcast new block
        // block_tx.send(block)?;

        Ok(())
    }
}

impl<E> BackfilledBlockStream<E>
where
    E: EnvironmentKind,
{
    pub fn next_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::result::Result<Option<pb::ConnectResponse>, Status> {
        if self.block < self.indexed {
            self.next_backfilled_block().map(Some)
        } else {
            todo!()
        }
    }

    fn next_backfilled_block(&mut self) -> std::result::Result<pb::ConnectResponse, Status> {
        // backfill
        let block = self
            .chain
            .block_by_number(self.block)
            .map_err(|_| Status::internal("failed to load data"))?
            .ok_or_else(|| Status::unavailable("not started indexing"))?;
        self.block += 1;
        let mut buf = Vec::new();
        buf.reserve(block.encoded_len());
        block
            .encode(&mut buf)
            .map_err(|_| Status::internal("failed to encode message"))?;
        let inner_data = prost_types::Any {
            type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Block".to_string(),
            value: buf,
        };
        let data = pb::Data {
            sequence: block.block_number,
            data: Some(inner_data),
        };
        let message = pb::connect_response::Message::Data(data);
        Ok(pb::ConnectResponse {
            message: Some(message),
        })
    }
}

impl<E> Stream for BackfilledBlockStream<E>
where
    E: EnvironmentKind,
{
    type Item = std::result::Result<pb::ConnectResponse, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.next_block(cx) {
            Ok(None) => Poll::Pending,
            Ok(Some(response)) => Poll::Ready(Some(Ok(response))),
            Err(err) => Poll::Ready(Some(Err(err))),
        }
    }
}
