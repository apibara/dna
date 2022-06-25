use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Error, Result};
use futures::{future, Stream, StreamExt};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

use crate::{
    chain::{BlockEvents, BlockHash, BlockHeader, ChainProvider, EventFilter},
    head_tracker::{HeadTracker, Message as HeadMessage},
    persistence::Id,
};

use super::persistence::{IndexerPersistence, State as IndexerState};

/// A message in `IndexerStream`.
#[derive(Debug)]
pub enum Message {
    /// Client connected.
    Connected(IndexerState),
    /// A new block was produced, increasing the chain height.
    NewBlock(BlockHeader),
    /// A chain reorganization occurred.
    Reorg(BlockHeader),
    /// New events produced.
    NewEvents(BlockEvents),
}

#[derive(Debug)]
pub enum ClientToIndexerMessage {
    Connect(Id),
    AckBlock(BlockHash),
}

/// A stream of `Message`.
pub type IndexerStream = ReceiverStream<Result<Message>>;

/// An indexer task.
pub type IndexerHandle = JoinHandle<Result<()>>;

pub async fn start_indexer<P, CS, IP>(
    indexer_state: &IndexerState,
    client_stream: CS,
    provider: Arc<P>,
    indexer_persistence: Arc<IP>,
) -> Result<(IndexerHandle, IndexerStream)>
where
    P: ChainProvider,
    CS: Stream<Item = Result<ClientToIndexerMessage>> + Send + 'static,
    IP: IndexerPersistence,
{
    let (indexer_service, stream_rx) =
        IndexerService::new(indexer_state, provider, indexer_persistence, client_stream);

    let handle = tokio::spawn(indexer_service.run());
    let stream = ReceiverStream::new(stream_rx);

    Ok((handle, stream))
}

struct IndexerService<P: ChainProvider, IP: IndexerPersistence> {
    provider: Arc<P>,
    persistence: Arc<IP>,
    indexer_id: Id,
    stream_tx: mpsc::Sender<Result<Message>>,
    client_stream: Pin<Box<dyn Stream<Item = Result<ClientToIndexerMessage>> + Send + 'static>>,
    block_cache: HashMap<u64, BlockHeader>,
    // filters: Vec<EventFilter>,
    block_batch_size: u64,
    next_block_number: u64,
    head: Option<BlockHeader>,
}

impl<P, IP> IndexerService<P, IP>
where
    P: ChainProvider,
    IP: IndexerPersistence,
{
    pub fn new<CS>(
        state: &IndexerState,
        provider: Arc<P>,
        persistence: Arc<IP>,
        client_stream: CS,
    ) -> (IndexerService<P, IP>, mpsc::Receiver<Result<Message>>)
    where
        CS: Stream<Item = Result<ClientToIndexerMessage>> + Send + 'static,
    {
        let (stream_tx, stream_rx) = mpsc::channel(64);

        let next_block_number = state.indexed_to_block.map(|b| b + 1).unwrap_or(0);
        let service = IndexerService {
            provider,
            persistence,
            indexer_id: state.id.clone(),
            stream_tx,
            client_stream: Box::pin(client_stream),
            next_block_number,
            block_batch_size: state.block_batch_size as u64,
            block_cache: HashMap::new(),
            head: None,
        };
        (service, stream_rx)
    }

    pub async fn run(mut self) -> Result<()> {
        let head_tracker = HeadTracker::new(self.provider.clone());
        let (head_tracker_handle, head_stream) = head_tracker
            .start()
            .await
            .context("failed to start head tracker")?;

        tokio::pin!(head_stream);
        tokio::pin!(head_tracker_handle);

        info!(indexer_id=?self.indexer_id.to_str(), "starting indexer service");

        // notify client that it was successfully connected
        let initial_state = self
            .persistence
            .get_indexer(&self.indexer_id)
            .await?
            .ok_or(Error::msg("indexer not found"))?;

        self.stream_tx
            .send(Ok(Message::Connected(initial_state)))
            .await?;

        let loop_sleep_duration = Duration::from_millis(500);

        let filters: Vec<EventFilter> = Vec::new();
        let mut waiting_for_ack: Option<BlockHash> = None;
        let mut block_batch = VecDeque::new();
        loop {
            tokio::select! {
                // poll in order since the code needs to know about
                // new heads.
                biased;

                ret = &mut head_tracker_handle => {
                    error!("head tracker service stopped: {:?}", ret);
                    return Err(Error::msg("head tracker service stopped"))
                }

                Some(msg) = head_stream.next() => {
                    match msg {
                        HeadMessage::NewBlock(block) => {
                            info!("⛏️ {} {}", block.number, block.hash);
                            self.update_with_new_block(block.clone())?;
                            self.stream_tx.send(Ok(Message::NewBlock(block))).await?;
                            continue
                        }
                        HeadMessage::Reorg(block) => {
                            info!("🤕 {} {}", block.number, block.hash);
                            self.update_with_reorg_block(block.clone())?;
                            self.stream_tx.send(Ok(Message::Reorg(block))).await?;
                            continue
                        }
                    }
                }

                Some(msg) = self.client_stream.next(), if waiting_for_ack.is_some() => {
                    todo!()
                }

                _ = future::ready(()) => {
                    // no new head or message from the client.
                    // continue by sending new events to the client.
                }
            }

            match self.head {
                None => continue,
                Some(ref head) => {
                    if head.number < self.next_block_number {
                        info!(head=?head.number, "at head of chain");
                        tokio::time::sleep(loop_sleep_duration).await;
                        continue;
                    }
                    // can now send the next events to the client
                    info!("house keeping done. send next block events");
                    if block_batch.is_empty() {
                        let (start, end) = self.next_block_events_query()?;
                        info!("get events: [{}, {}]", start, end);

                        // TODO: join block events by different filters
                        if filters.len() > 1 {
                            return Err(Error::msg("support only one filter for now"));
                        }

                        for filter in &filters {
                            let block_events = self
                                .provider
                                .get_events_in_range(start, end, filter)
                                .await
                                .context("failed to fetch events")?;
                            block_batch.extend(block_events.into_iter());
                        }
                        self.update_next_block(end + 1);
                    }

                    match block_batch.pop_front() {
                        None => {
                            // no events in this block range
                        }
                        Some(block_events) => {
                            waiting_for_ack = Some(block_events.hash.clone());

                            self.stream_tx
                                .send(Ok(Message::NewEvents(block_events)))
                                .await?;
                        }
                    }
                }
            }
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
