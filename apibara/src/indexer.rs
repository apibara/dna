//! Produce a stream of events/logs.
use anyhow::{Context, Error, Result};
use futures::{Future, Stream};
use once_cell::sync::Lazy;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{debug, error, trace};

use crate::chain::{BlockEvents, BlockHeader, ChainProvider, EventFilter};

static CLIENT_SERVER_MESSAGE_TIMEOUT: Lazy<Duration> = Lazy::new(|| Duration::from_millis(5000));

const BLOCK_EVENTS_MAX_RANGE: u64 = 50;

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub from_block: u64,
    pub filters: Vec<EventFilter>,
}

pub fn start<P: ChainProvider>(
    provider: Arc<P>,
    config: IndexerConfig,
) -> (IndexerClient, IndexerStream<P>) {
    let (tx, rx) = mpsc::channel(64);
    let client = IndexerClient { tx };
    let stream = IndexerStream::new(rx, provider, config.from_block, config.filters);
    (client, stream)
}

type UpdateHeadResponse = Result<()>;

#[derive(Debug)]
pub(crate) enum IndexerClientStreamMessage {
    UpdateHead(BlockHeader, oneshot::Sender<UpdateHeadResponse>),
}

#[derive(Debug, Clone)]
pub struct IndexerClient {
    tx: mpsc::Sender<IndexerClientStreamMessage>,
}

impl IndexerClient {
    pub async fn update_head(&self, head: BlockHeader) -> UpdateHeadResponse {
        let (reply_tx, reply_rx) = oneshot::channel();
        let message = IndexerClientStreamMessage::UpdateHead(head, reply_tx);
        if let Err(_) = self.tx.send(message).await {
            return Err(Error::msg("failed to update head"));
        }
        timeout(*CLIENT_SERVER_MESSAGE_TIMEOUT, reply_rx)
            .await
            .context("update head timeout")?
            .context("failed to receive update head response")?
    }
}

struct IndexerStreamState<P: ChainProvider> {
    provider: Arc<P>,
    sync_block: u64,
    next_sync_block: u64,
    current_head: Option<BlockHeader>,
    filters: Vec<EventFilter>,
    loaded_block_events: VecDeque<BlockEvents>,
    get_events_by_block_fut: Option<Pin<Box<dyn Future<Output = Result<Vec<BlockEvents>>> + Send>>>,
}

pub struct IndexerStream<P: ChainProvider> {
    rx: mpsc::Receiver<IndexerClientStreamMessage>,
    state: IndexerStreamState<P>,
}

impl<P: ChainProvider> IndexerStream<P> {
    pub(crate) fn new(
        rx: mpsc::Receiver<IndexerClientStreamMessage>,
        provider: Arc<P>,
        from_block: u64,
        filters: Vec<EventFilter>,
    ) -> IndexerStream<P> {
        // u64::MAX means starting from block 0
        let sync_block = if from_block == 0 {
            u64::MAX
        } else {
            from_block - 1
        };
        let state = IndexerStreamState {
            provider,
            sync_block,
            next_sync_block: u64::MAX,
            current_head: None,
            filters,
            loaded_block_events: VecDeque::new(),
            get_events_by_block_fut: None,
        };
        IndexerStream { rx, state }
    }
}

impl<P: ChainProvider> Stream for IndexerStream<P> {
    type Item = BlockEvents;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // poll incoming client message until Poll::Pending so that the stream
            // will be waken up automatically on new messages
            match self.rx.poll_recv(cx) {
                Poll::Ready(None) => {
                    error!("client rx closed");
                    return Poll::Ready(None);
                }
                // TODO: handle multiple messages at the same time?? Until rx is pending to avoid dying
                Poll::Ready(Some(msg)) => match update_state_from_message(&mut self.state, msg) {
                    Err(err) => {
                        error!("indexer stream error: {:?}", err);
                        return Poll::Ready(None);
                    }
                    Ok(_) => {}
                },
                Poll::Pending => break,
            };
        }

        // send loaded elements if any
        if let Some(next_block) = self.state.loaded_block_events.pop_front() {
            trace!("send loaded block events");
            return Poll::Ready(Some(next_block));
        }

        let new_get_events_by_block_fut =
            if let Some(mut get_events_by_block_fut) = self.state.get_events_by_block_fut.take() {
                trace!("poll get_events_by_block_fut");
                match get_events_by_block_fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(block_events)) => {
                        // TODO: properly advance sync state
                        for event in block_events {
                            self.state.loaded_block_events.push_back(event);
                        }
                        trace!("new sync block {}", self.state.next_sync_block);
                        self.state.sync_block = self.state.next_sync_block;
                        // signal to create new request
                        None
                    }
                    Poll::Ready(Err(err)) => {
                        // TODO: retry
                        error!("error fetching block events: {:?}", err);
                        return Poll::Ready(None);
                    }
                    Poll::Pending => Some(get_events_by_block_fut),
                }
            } else {
                // signal to create new request
                None
            };

        self.state.get_events_by_block_fut = match new_get_events_by_block_fut {
            Some(fut) => Some(fut),
            None => {
                trace!("create get_events_by_block_fut");
                let current_head = self.state.current_head.clone();
                let sync_block = self.state.sync_block;
                // TODO: support reorg detection?
                if let Some(current_head) = current_head {
                    if sync_block != u64::MAX && current_head.number <= sync_block {
                        trace!("skip {} {}", current_head.number, sync_block);
                        None
                    } else if current_head.number - 1 == sync_block {
                        // get by hash
                        trace!("get events by hash {:?}", current_head.hash);
                        self.state.next_sync_block = current_head.number;
                        let fut = self.state.provider.get_events_by_block_hash(
                            &current_head.hash,
                            &self.state.filters,
                        );
                        // schedule polling immediately after this
                        cx.waker().wake_by_ref();
                        Some(fut)
                    } else {
                        // get by range
                        let from_block = if self.state.sync_block == u64::MAX {
                            0
                        } else {
                            self.state.sync_block + 1
                        };
                        let to_block = std::cmp::min(
                            from_block + BLOCK_EVENTS_MAX_RANGE - 1,
                            current_head.number - 1,
                        );
                        trace!("get events in range [{}, {}]", from_block, to_block);
                        self.state.next_sync_block = to_block;
                        let fut = self.state.provider.get_events_by_block_range(
                            from_block,
                            to_block,
                            &self.state.filters,
                        );
                        // schedule polling immediately after this
                        cx.waker().wake_by_ref();
                        Some(fut)
                    }
                } else {
                    None
                }
            }
        };

        // nothing left to do
        Poll::Pending
    }
}

fn update_state_from_message<P: ChainProvider>(
    mut state: &mut IndexerStreamState<P>,
    message: IndexerClientStreamMessage,
) -> Result<()> {
    match message {
        IndexerClientStreamMessage::UpdateHead(new_head, reply_tx) => {
            debug!(
                "update indexer head {:?} -> {} {}",
                state.current_head.as_ref().map(|h| h.number),
                new_head.number,
                new_head.hash
            );
            state.current_head = Some(new_head);
            if let Err(_) = reply_tx.send(Ok(())) {
                Err(Error::msg("failed to respond to UpdateHead"))
            } else {
                Ok(())
            }
        }
    }
}
