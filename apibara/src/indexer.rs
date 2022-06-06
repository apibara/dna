//! Produce a stream of events/logs.
use anyhow::{Context, Error, Result};
use futures::Stream;
use once_cell::sync::Lazy;
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{debug, error, info};

use crate::chain::{BlockEvents, BlockHash, BlockHeader, ChainProvider};

static CLIENT_SERVER_MESSAGE_TIMEOUT: Lazy<Duration> = Lazy::new(|| Duration::from_millis(5000));

pub fn start<P: ChainProvider>(
    provider: Arc<P>,
    from_block: u64,
) -> (IndexerClient, IndexerStream<P>) {
    let (tx, rx) = mpsc::channel(64);
    let client = IndexerClient { tx };
    let stream = IndexerStream::new(rx, provider, from_block);
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

#[derive(Debug)]
struct IndexerStreamState<P: ChainProvider> {
    provider: Arc<P>,
    sync_block: u64,
    current_head: Option<BlockHeader>,
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
            current_head: None,
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

        // TODO:
        let e = BlockEvents {
            number: 0,
            hash: BlockHash([0; 32]),
            events: Vec::new(),
        };
        // Poll::Ready(Some(e))
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
