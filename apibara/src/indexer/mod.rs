//! Index on-chain data.
use anyhow::{Context, Error, Result};
use futures::{future, StreamExt};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

use crate::{
    chain::{BlockEvents, BlockHeader, ChainProvider, EventFilter},
    head_tracker::{HeadTracker, Message as HeadMessage},
};

/// A single indexer configuration.
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Start indexing from this block.
    pub from_block: u64,
    /// Index these events.
    pub filters: Vec<EventFilter>,
}

/// A message in `IndexerStream`.
#[derive(Debug)]
pub enum Message {
    /// A new block was produced, increasing the chain height.
    NewBlock(BlockHeader),
    /// A chain reorganization occurred.
    Reorg(BlockHeader),
    /// New events produced.
    NewEvents(BlockEvents),
}

/// A stream of `Message`.
pub type IndexerStream = ReceiverStream<Message>;

/// An indexer task.
pub type IndexerHandle = JoinHandle<Result<()>>;

pub struct Indexer<P: ChainProvider> {
    provider: Arc<P>,
    from_block: u64,
    filters: Vec<EventFilter>,
}

struct IndexerLoop<P: ChainProvider> {
    provider: Arc<P>,
    stream_tx: mpsc::Sender<Message>,
    block_cache: HashMap<u64, BlockHeader>,
    filters: Vec<EventFilter>,
    block_batch_size: u64,
    next_block_number: u64,
    head: Option<BlockHeader>,
}

impl IndexerConfig {
    pub fn new(from_block: u64) -> Self {
        IndexerConfig {
            from_block,
            filters: Vec::new(),
        }
    }

    pub fn add_filter(mut self, filter: EventFilter) -> Self {
        self.filters.push(filter);
        self
    }
}

impl<P: ChainProvider> Indexer<P> {
    pub fn new(provider: Arc<P>, from_block: u64, filters: Vec<EventFilter>) -> Indexer<P> {
        Indexer {
            provider,
            from_block,
            filters,
        }
    }

    /// Start the indexer.
    pub fn start(self) -> Result<(IndexerHandle, IndexerStream)> {
        // TODO: configure block batch size
        let block_batch_size = 200;

        let (indexer_loop, stream_rx) = IndexerLoop::new(
            self.provider,
            self.from_block,
            self.filters,
            block_batch_size,
        );
        let handle = tokio::spawn(indexer_loop.run_loop());
        let stream = ReceiverStream::new(stream_rx);

        Ok((handle, stream))
    }
}

impl<P: ChainProvider> IndexerLoop<P> {
    pub fn new(
        provider: Arc<P>,
        from_block: u64,
        filters: Vec<EventFilter>,
        block_batch_size: usize,
    ) -> (Self, mpsc::Receiver<Message>) {
        let (stream_tx, stream_rx) = mpsc::channel(block_batch_size * 2);
        let indexer = IndexerLoop {
            provider,
            stream_tx,
            filters,
            next_block_number: from_block,
            block_batch_size: block_batch_size as u64,
            block_cache: HashMap::new(),
            head: None,
        };
        (indexer, stream_rx)
    }

    pub async fn run_loop(mut self) -> Result<()> {
        let head_tracker = HeadTracker::new(self.provider.clone());
        let (head_tracker_handle, head_stream) = head_tracker
            .start()
            .await
            .context("failed to start head tracker")?;

        tokio::pin!(head_stream);
        tokio::pin!(head_tracker_handle);

        info!("starting indexer service");
        loop {
            tokio::select! {
                // Poll in order so that the busy work is only done if there
                // is no new head and the head tracker is still running.
                biased;

                // TODO: check return value and wrap error
                _ = &mut head_tracker_handle => {
                    error!("head tracker service stopped");
                    return Err(Error::msg("head tracker service stopped"))
                }
                Some(msg) = head_stream.next() => {
                    match msg {
                        HeadMessage::NewBlock(block) => {
                            info!("⛏️ {} {}", block.number, block.hash);
                            self.update_with_new_block(block.clone())?;
                            self.stream_tx.send(Message::NewBlock(block)).await?;
                        }
                        HeadMessage::Reorg(block) => {
                            info!("🤕 {} {}", block.number, block.hash);
                            self.update_with_reorg_block(block.clone())?;
                            self.stream_tx.send(Message::Reorg(block)).await?;
                        }
                    }
                }
                _ = future::ready(()), if self.is_ready() => {
                    let duration = std::time::Duration::from_millis(5);
                    tokio::time::sleep(duration).await;
                    // starknet does not support fetching events by block
                    // hash so no point in detecting being close to head
                    // TODO: handle that case for other chains
                    let (start, end) = self.next_block_events_query()?;
                    info!("get events: [{}, {}]", start, end);

                    // TODO: join block events by different filters
                    if self.filters.len() > 1 {
                        return Err(Error::msg("support only one filter for now"))
                    }

                    for filter in &self.filters {
                        let block_events = self.provider.get_events_in_range(start, end, filter).await.expect("get events");
                        for event in block_events {
                            self.stream_tx.send(Message::NewEvents(event)).await?;
                        }
                    }
                    self.update_next_block(end + 1);
                }
            }
        }
    }

    pub fn is_ready(&self) -> bool {
        match self.head {
            None => false,
            Some(ref head) => self.next_block_number <= head.number,
        }
    }

    pub fn update_with_new_block(&mut self, new_block: BlockHeader) -> Result<()> {
        if new_block.number < self.next_block_number {
            return Err(Error::msg("new block is not valid"));
        }
        if self.block_cache.contains_key(&new_block.number) {
            return Err(Error::msg("duplicate block"));
        }
        self.block_cache.insert(new_block.number, new_block.clone());
        self.head = Some(new_block);
        Ok(())
    }

    pub fn update_with_reorg_block(&mut self, new_block: BlockHeader) -> Result<()> {
        if let Some(old_head) = self.head.take() {
            // invalidate all blocks between old head and new head
            for block_number in new_block.number..old_head.number {
                self.block_cache.remove(&block_number);
            }
        }
        self.next_block_number = u64::min(self.next_block_number, new_block.number);
        self.block_cache.insert(new_block.number, new_block.clone());
        self.head = Some(new_block);
        Ok(())
    }

    pub fn next_block_events_query(&self) -> Result<(u64, u64)> {
        match self.head {
            None => Err(Error::msg("must have head to fetch block events")),
            Some(ref head) => {
                let start = self.next_block_number;
                let end = u64::min(start + self.block_batch_size - 1, head.number);
                Ok((start, end))
            }
        }
    }

    pub fn update_next_block(&mut self, next_block: u64) {
        self.next_block_number = next_block;
    }
}
