use std::{collections::HashMap, pin::Pin, sync::Arc};

use anyhow::{Context, Error, Result};
use futures::{future, Stream, StreamExt};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info};

use crate::{
    chain::{BlockEvents, BlockHash, BlockHeader, ChainProvider, EventFilter},
    head_tracker::HeadTracker,
    persistence::Id,
};

use super::persistence::{IndexerPersistence, State as IndexerState};

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

        loop {
            // 1. update current head
            // 2. check head tracker did not stop
            // 3. waiting for ack?
            //   a. yes, then wait for it
            //   b. no, then send next block data
        }
    }
}
