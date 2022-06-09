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
    /// New block produced.
    NewBlock(BlockHeader),
    /// Chain reorganization.
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
                    block_buffer.push_back(block);
                }
            }
        }

        Ok(block_buffer)
    }

    async fn run_loop(self, _tx: mpsc::Sender<Message>) -> Result<()> {
        let mut _block_buffer = self.build_chain_reorg_buffer().await?;
        let mut block_stream = self.provider.subscribe_blocks()?;
        while let Some(_block) = block_stream.next().await {
            debug!("received block");
        }
        Ok(())
    }
}
