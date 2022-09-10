//! Ingest blocks from the node.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use apibara_node::{
    chain_tracker::{ChainChange, ChainTracker, ChainTrackerError},
    db::libmdbx::EnvironmentKind,
};
use starknet::providers::SequencerGatewayProvider;
use tokio::sync::{
    broadcast::{
        self,
        error::{SendError, TryRecvError},
    },
    mpsc,
};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{debug, error, info};

use crate::{
    block_builder::{BlockBuilder, BlockBuilderError},
    core::Block,
};

#[derive(Debug, Clone)]
pub enum BlockStreamMessage {
    Data(Box<Block>),
    Reorg(u64),
}

pub struct BlockIngestor<E: EnvironmentKind> {
    chain: Arc<ChainTracker<Block, E>>,
    block_builder: BlockBuilder,
    block_tx: broadcast::Sender<BlockStreamMessage>,
    _block_rx: broadcast::Receiver<BlockStreamMessage>,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockIngestorError {
    #[error("error tracking chain state")]
    ChainTracker(#[from] ChainTrackerError),
    #[error("error fetching or parsing block")]
    BlockBuilder(#[from] BlockBuilderError),
    #[error("error broadcasting chain event")]
    Broadcast(#[from] SendError<BlockStreamMessage>),
    #[error("chain not started syncing")]
    EmptyChain,
}

pub type Result<T> = std::result::Result<T, BlockIngestorError>;

const MESSAGE_CHANNEL_SIZE: usize = 128;

pub struct BackfilledBlockStreamer<E: EnvironmentKind> {
    tx: mpsc::Sender<std::result::Result<BlockStreamMessage, Status>>,
    chain: Arc<ChainTracker<Block, E>>,
    block: u64,
    indexed: u64,
    rx: broadcast::Receiver<BlockStreamMessage>,
    buffer: VecDeque<BlockStreamMessage>,
    ct: CancellationToken,
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
        let (block_tx, block_rx) = broadcast::channel(MESSAGE_CHANNEL_SIZE);

        Ok(BlockIngestor {
            chain,
            block_builder,
            block_tx,
            _block_rx: block_rx,
        })
    }

    /// Subscribe to new chain blocks and reorganizations.
    pub fn subscribe(&self) -> broadcast::Receiver<BlockStreamMessage> {
        self.block_tx.subscribe()
    }

    pub async fn stream_from_sequence(
        &self,
        starting_sequence: u64,
        tx: mpsc::Sender<std::result::Result<BlockStreamMessage, Status>>,
        ct: CancellationToken,
    ) -> Result<BackfilledBlockStreamer<E>> {
        let indexed = self
            .chain
            .latest_indexed_block()?
            .ok_or(BlockIngestorError::EmptyChain)?
            .block_number;
        let rx = self.subscribe();

        info!(start = %starting_sequence, "start stream");

        Ok(BackfilledBlockStreamer {
            tx,
            rx,
            chain: self.chain.clone(),
            block: starting_sequence,
            indexed,
            buffer: VecDeque::default(),
            ct,
        })
    }

    pub async fn start(&self, ct: CancellationToken) -> Result<()> {
        let current_head = self
            .block_builder
            .latest_block_with_backoff(ct.clone())
            .await?;
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

        let mut head_refreshed_at = chrono::offset::Utc::now();

        let far_head_refresh_interval =
            chrono::Duration::from_std(Duration::from_secs(60)).expect("duration conversion");
        let close_head_refresh_interval =
            chrono::Duration::from_std(Duration::from_secs(10)).expect("duration conversion");
        let sync_sleep_interval = Duration::from_secs(5);

        loop {
            if ct.is_cancelled() {
                break;
            }

            match self.chain.gap()? {
                None => {
                    current_block_number = self
                        .fetch_and_broadcast_block(current_block_number, &ct)
                        .await?;
                }
                Some(0) => {
                    let head_height = self
                        .chain
                        .head_height()?
                        .ok_or(BlockIngestorError::EmptyChain)?;
                    current_block_number = self.fetch_and_broadcast_latest_block(&ct).await?;
                    if current_block_number == head_height + 1 {
                        tokio::time::sleep(sync_sleep_interval).await;
                    }
                }
                Some(gap) => {
                    let head_refresh_elapsed = chrono::offset::Utc::now() - head_refreshed_at;
                    let should_refresh_head = (gap > 50
                        && head_refresh_elapsed > far_head_refresh_interval)
                        || (gap > 10 && head_refresh_elapsed > close_head_refresh_interval);
                    if should_refresh_head {
                        debug!("refresh head");
                        let current_head = self
                            .block_builder
                            .latest_block_with_backoff(ct.clone())
                            .await?;
                        self.chain.update_head(&current_head)?;
                        head_refreshed_at = chrono::offset::Utc::now();
                    }
                    current_block_number = self
                        .fetch_and_broadcast_block(current_block_number, &ct)
                        .await?;
                }
            }
        }

        Ok(())
    }
    #[tracing::instrument(skip(self, ct))]
    async fn fetch_and_broadcast_latest_block(&self, ct: &CancellationToken) -> Result<u64> {
        let block = tokio::select! {
            block = self.block_builder.latest_block_with_backoff(ct.clone()) => {
                block?
            }
            _ = ct.cancelled() => {
                return Ok(0)
            }
        };

        self.apply_block(block)
    }

    #[tracing::instrument(skip(self, ct))]
    async fn fetch_and_broadcast_block(
        &self,
        block_number: u64,
        ct: &CancellationToken,
    ) -> Result<u64> {
        let block = tokio::select! {
            block = self.block_builder.block_by_number_with_backoff(block_number, ct.clone()) => {
                block?
            }
            _ = ct.cancelled() => {
                return Ok(0)
            }
        };

        self.apply_block(block)
    }

    fn apply_block(&self, block: Block) -> Result<u64> {
        info!(block_number = ?block.block_number, "got block");
        let block_number = block.block_number;

        match self.chain.update_indexed_block(block)? {
            ChainChange::Advance(blocks) => {
                info!("chain advanced by {} blocks", blocks.len());
                let mut next_block_number = block_number + 1;
                for block in blocks {
                    next_block_number = block.block_number + 1;
                    self.block_tx
                        .send(BlockStreamMessage::Data(Box::new(block)))?;
                }
                Ok(next_block_number)
            }
            ChainChange::Reorg(blocks) => {
                info!("chain reorged by {} blocks", blocks.len());
                todo!()
            }
            ChainChange::MissingBlock(block_number, block_hash) => {
                info!("block is missing: {}/{}", block_number, block_hash);
                todo!()
            }
            ChainChange::AlreadySeen => {
                info!("block already seen");
                Ok(block_number + 1)
            }
        }
    }
}

impl<E> BackfilledBlockStreamer<E>
where
    E: EnvironmentKind,
{
    pub async fn start(&mut self) -> std::result::Result<(), Status> {
        loop {
            if self.ct.is_cancelled() {
                return Ok(());
            }
            // send backlog in batches
            self.send_next_block_batch().await?;

            // check for new live blocks
            match self.rx.try_recv() {
                Err(TryRecvError::Empty) => (),
                Err(_) => {
                    return Err(Status::internal(
                        "failed to communicate with block ingestor",
                    ))
                }
                Ok(BlockStreamMessage::Reorg(invalidate_after)) => {
                    if invalidate_after > self.indexed {
                        // TODO: drop messages that were invalidated
                        todo!()
                    }
                    todo!()
                }
                Ok(BlockStreamMessage::Data(block)) => {
                    info!(block_number = %block.block_number, "received live block data");
                    self.buffer.push_front(BlockStreamMessage::Data(block));

                    // keep buffer reasonably sized
                    while self.buffer.len() > 50 {
                        self.buffer.pop_front();
                    }

                    // keep tracking the first "live" block to send
                    if let Some(BlockStreamMessage::Data(block)) = self.buffer.front() {
                        self.indexed = block.block_number;
                    }
                }
            }

            if self.block >= self.indexed {
                info!("finished sending backfilled blocks");
                break;
            }
        }

        while let Some(block) = self.buffer.pop_front() {
            if self.ct.is_cancelled() {
                return Ok(());
            }
            self.tx
                .send(Ok(block))
                .await
                .map_err(|_| Status::internal("failed to stream block"))?;
        }
        info!("finished sending buffer");

        loop {
            if self.ct.is_cancelled() {
                return Ok(());
            }
            // send live events
            let block = self
                .rx
                .recv()
                .await
                .map_err(|_| Status::internal("failed to stream block"))?;
            self.tx
                .send(Ok(block))
                .await
                .map_err(|_| Status::internal("failed to stream block"))?;
        }
    }

    pub async fn send_next_block_batch(&mut self) -> std::result::Result<(), Status> {
        for _ in 1..100 {
            let block = self.next_backfilled_block()?;
            self.tx
                .send(Ok(block))
                .await
                .map_err(|_| Status::internal("failed to stream block"))?;
            if self.block >= self.indexed {
                return Ok(());
            }
        }
        Ok(())
    }

    fn next_backfilled_block(&mut self) -> std::result::Result<BlockStreamMessage, Status> {
        // backfill
        let block = self
            .chain
            .block_by_number(self.block)
            .map_err(|_| Status::internal("failed to load data"))?
            .ok_or_else(|| Status::unavailable("not started indexing"))?;
        self.block += 1;
        Ok(BlockStreamMessage::Data(Box::new(block)))
    }
}
