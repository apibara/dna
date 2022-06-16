//! Service that tracks the chain head.
use anyhow::{Error, Result};
use futures::StreamExt;
use std::{collections::VecDeque, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::debug;

use crate::chain::{BlockHeader, ChainProvider};

const REORG_BUFFER_SIZE: usize = 32;
const CHANNEL_BUFFER_SIZE: usize = 64;

/// Message sent by the head tracker.
#[derive(Debug, Clone)]
pub enum Message {
    /// New block produced. This is now the head.
    NewBlock(BlockHeader),
    /// Chain reorganization. The specified block is the new head.
    Reorg(BlockHeader),
}

pub struct HeadTracker<P: ChainProvider> {
    provider: Arc<P>,
    reorg_buffer_size: usize,
}

impl<P: ChainProvider> HeadTracker<P> {
    pub fn new(provider: Arc<P>) -> Self {
        HeadTracker {
            provider,
            reorg_buffer_size: REORG_BUFFER_SIZE,
        }
    }

    pub async fn start(self) -> Result<(JoinHandle<Result<()>>, ReceiverStream<Message>)> {
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        let join_handle = tokio::spawn(self.run_loop(tx));

        let stream = ReceiverStream::new(rx);
        Ok((join_handle, stream))
    }

    async fn run_loop(self, tx: mpsc::Sender<Message>) -> Result<()> {
        let mut block_buffer = self.build_chain_reorg_buffer().await?;
        let mut block_stream = self.provider.subscribe_blocks()?;

        // send first block
        match block_buffer.front() {
            None => return Err(Error::msg("head tracker buffer is empty")),
            Some(block) => {
                tx.send(Message::NewBlock(block.clone())).await?;
            }
        }

        while let Some(block) = block_stream.next().await {
            debug!(
                "received new block {} {} (p: {:?})",
                block.number, block.hash, block.parent_hash
            );
            let prev_head = match block_buffer.front() {
                None => return Err(Error::msg("head tracker buffer is empty")),
                Some(prev_head) if prev_head.hash == block.hash => {
                    debug!("skip: block already seen");
                    // stream sent the same block twice
                    continue;
                }
                Some(prev_head) => prev_head.clone(),
            };

            debug!("prev head: {} {}", prev_head.number, prev_head.hash);
            if block
                .parent_hash
                .clone()
                .map(|h| h == prev_head.hash)
                .unwrap_or(false)
            {
                debug!("clean head application");
                // push to the _front_ of the queue
                block_buffer.push_front(block.clone());

                // only empty buffer every now and then
                while block_buffer.len() > 2 * self.reorg_buffer_size {
                    block_buffer.pop_back();
                }

                // send new head
                tx.send(Message::NewBlock(block)).await?;
            } else if block.number > prev_head.number + 1 {
                panic!("head application with gaps not handled");
            } else {
                panic!("head application rollback not handled");
            }
        }
        Ok(())
    }

    async fn build_chain_reorg_buffer(&self) -> Result<VecDeque<BlockHeader>> {
        let current_head = self.provider.get_head_block().await?;

        let mut next_hash = current_head.parent_hash.clone();
        let mut block_buffer = VecDeque::new();
        block_buffer.push_back(current_head);

        while block_buffer.len() < self.reorg_buffer_size {
            match next_hash {
                None => {
                    // genesis block
                    break;
                }
                Some(ref hash) => {
                    let block = self
                        .provider
                        .get_block_by_hash(hash)
                        .await?
                        .ok_or_else(|| {
                            Error::msg(format!("expected block with hash {} to exist", hash))
                        })?;
                    next_hash = block.parent_hash.clone();

                    // push to the _back_ of the queue because the loop is walking the
                    // chain backwards.
                    block_buffer.push_back(block);
                }
            }
        }

        Ok(block_buffer)
    }
}
