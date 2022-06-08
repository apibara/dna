use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::{future, Future};
use starknet::core::types::FieldElement;
use starknet_rpc::{Block, BlockHash as StarkNetBlockHash, RpcProvider};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

use crate::chain::{
    BlockEvents, BlockHash, BlockHeader, BlockHeaderStream, ChainProvider, EventFilter,
};

pub struct StarkNetChainProvider {
    client: Arc<RpcProvider>,
    block_poll_interval: Duration,
}

impl StarkNetChainProvider {
    pub fn new(client: RpcProvider) -> Self {
        StarkNetChainProvider {
            client: Arc::new(client),
            block_poll_interval: Duration::from_millis(500),
        }
    }
}

#[async_trait]
impl ChainProvider for StarkNetChainProvider {
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let block = self
            .client
            .get_block_by_hash(&StarkNetBlockHash::Latest)
            .await
            .context("failed to fetch starknet latest block")?;
        Ok(starknet_block_to_block_header(block))
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let hash_fe =
            FieldElement::from_bytes_be(&hash.0).context("failed to convert block hash")?;
        let hash = StarkNetBlockHash::Hash(hash_fe);
        // FIXME: should the rpc return None if the block is missing?
        let block = self
            .client
            .get_block_by_hash(&hash)
            .await
            .context("failed to fetch starknet block by hash")?;
        Ok(Some(starknet_block_to_block_header(block)))
    }

    async fn blocks_subscription(&self) -> Result<BlockHeaderStream> {
        // starknet rpc does not support streaming blocks, so spawn a new thread
        // and keep polling the rpc endpoint.
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn({
            let client = self.client.clone();
            let block_poll_interval = self.block_poll_interval.clone();
            async move {
                let hash = StarkNetBlockHash::Latest;
                // track last sent block to avoid sending the same block multiple times
                let mut prev_block_hash: Option<BlockHash> = None;
                loop {
                    // FIXME: handle retry on error
                    let block = client.get_block_by_hash(&hash).await;
                    match block {
                        Ok(block) => {
                            let block = starknet_block_to_block_header(block);
                            let should_send = if let Some(ref prev) = prev_block_hash {
                                prev != &block.hash
                            } else {
                                true
                            };
                            if should_send {
                                prev_block_hash = Some(block.hash.clone());
                                if let Err(err) = tx.send(block).await {
                                    error!("failed to send starknet block to stream: {:?}", err);
                                }
                            }
                        }
                        Err(err) => {
                            error!("failed to fetch starknet block in stream: {:?}", err);
                        }
                    }
                    tokio::time::sleep(block_poll_interval).await;
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }

    fn get_events_by_block_range(
        &self,
        _from_block: u64,
        _to_block: u64,
        _filters: &[EventFilter],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BlockEvents>>> + Send>> {
        error!("TODO get_events_by_block_block_range");
        Box::pin(future::ready(Ok(vec![])))
    }

    fn get_events_by_block_hash(
        &self,
        _hash: &BlockHash,
        _filters: &[EventFilter],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BlockEvents>>> + Send>> {
        error!("TODO get_events_by_block_hash");
        Box::pin(future::ready(Ok(vec![])))
    }
}

fn starknet_block_to_block_header(block: Block) -> BlockHeader {
    let hash = BlockHash(block.block_hash.to_bytes_be());
    let parent_hash = Some(BlockHash(block.parent_hash.to_bytes_be()));
    BlockHeader {
        hash,
        parent_hash,
        number: block.block_number,
        timestamp: block.accepted_time,
    }
}
