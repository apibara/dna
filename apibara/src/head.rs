use anyhow::{Context, Error, Result};
use futures::{Future, Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Poll},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::error;

use crate::chain::{BlockHeader, ChainProvider};

pub async fn start<P>(provider: Arc<P>) -> Result<HeadTracker>
where
    P: ChainProvider + Send + Sync + 'static,
{
    let current = provider
        .get_head_block()
        .await
        .context("could not fetch initial block")?;

    // Work around lifetime of provider and subscription.
    let (block_stream_tx, block_stream_rx) = mpsc::channel(64);
    let block_stream_handle = tokio::spawn({
        let provider = provider.clone();
        async move {
            if let Ok(mut block_stream) = provider.blocks_subscription().await {
                while let Some(block) = block_stream.next().await {
                    if let Err(_) = block_stream_tx.send(block).await {
                        return ();
                    }
                }
            } else {
                return ();
            }
        }
    });

    Ok(HeadTracker {
        current,
        block_stream_handle,
        block_stream_rx,
    })
}

#[derive(Debug)]
pub enum BlockStreamMessage {
    NewBlock(BlockHeader),
    Rollback(BlockHeader),
}

/// Track the current blockchain head, creating
/// messages to signal either a new block, or
/// a chain rollback.
pub struct HeadTracker {
    block_stream_handle: JoinHandle<()>,
    block_stream_rx: mpsc::Receiver<BlockHeader>,
    current: BlockHeader,
}

impl Stream for HeadTracker {
    type Item = BlockStreamMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.block_stream_handle).poll(cx) {
            Poll::Ready(_) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        match Pin::new(&mut self.block_stream_rx).poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                error!("head tracker stream ended");
                Poll::Ready(None)
            }
            Poll::Ready(Some(block)) => {
                if is_clean_apply(&self.current, &block) {
                    self.current = block.clone();
                    Poll::Ready(Some(BlockStreamMessage::NewBlock(block)))
                } else {
                    // TODO: handle reorg better
                    // this code assumes that node gives us a (new) valid head and
                    // will continue from there.
                    // But that's not always the case, for example we can get
                    // the following stream:
                    // (a, 1) - (b, 2, p: a) - (c, 2, p: a) - (d, 3, p: b)
                    //
                    // notice how node `d` has `b` as parent. The current code
                    // emits the following sequence:
                    //
                    // NB(a) - NB(b) - R(c) - R(d)
                    //
                    // The correct sequence is:
                    //
                    // NB(a) - NB(b) - R(c) - R(b) - NB(d)
                    //
                    // Somehow we need to generate two events in one step.
                    self.current = block.clone();
                    Poll::Ready(Some(BlockStreamMessage::Rollback(block)))
                }
            }
        }
    }
}

fn is_clean_apply(current: &BlockHeader, new: &BlockHeader) -> bool {
    // current head and block have the same height
    if new.number == current.number {
        return new.hash == current.hash;
    }
    if new.number == current.number + 1 {
        return match &new.parent_hash {
            None => false,
            Some(hash) => hash == &current.hash,
        };
    }
    false
}
