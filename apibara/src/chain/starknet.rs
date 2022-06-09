//! StarkNet provider.
use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use backoff::ExponentialBackoff;
use futures::Stream;
use starknet::core::types::FieldElement;
use starknet_rpc::{Block as SNBlock, BlockHash as SNBlockHash, RpcProvider};
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace};

use crate::chain::{BlockHash, BlockHeader, ChainProvider};

#[derive(Debug)]
pub struct StarkNetProvider {
    client: Arc<RpcProvider>,
}

impl StarkNetProvider {
    pub fn new(url: &str) -> Result<StarkNetProvider> {
        let client = RpcProvider::new(url)?;
        Ok(StarkNetProvider {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl ChainProvider for StarkNetProvider {
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let block = self
            .client
            .get_block_by_hash(&SNBlockHash::Latest)
            .await
            .context("failed to fetch starknet head")?
            .ok_or_else(|| Error::msg("failed to fetch latest starknet block"))?;
        Ok(block.into())
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let hash_fe = hash
            .try_into()
            .context("failed to convert block hash to field element")?;
        let hash = SNBlockHash::Hash(hash_fe);
        let block = self
            .client
            .get_block_by_hash(&hash)
            .await
            .context("failed to fetch starknet head")?;
        Ok(block.map(Into::into))
    }

    fn subscribe_blocks(&self) -> Result<Pin<Box<dyn Stream<Item = BlockHeader> + Send>>> {
        // starknet rpc does not support subscriptions yet. for now, spawn a new task and
        // poll the rpc node.
        let (tx, rx) = mpsc::channel(64);
        let sleep_duration = Duration::from_millis(1_000);
        tokio::spawn({
            let client = self.client.clone();
            async move {
                let mut prev_block: Option<BlockHeader> = None;
                loop {
                    let block: Result<BlockHeader> =
                        backoff::future::retry(ExponentialBackoff::default(), || async {
                            trace!("fetching starknet head");
                            let block = client
                                .get_block_by_hash(&SNBlockHash::Latest)
                                .await
                                .context("failed to fetch starknet head")?
                                .ok_or_else(|| {
                                    Error::msg("failed to fetch latest starknet block")
                                })?;
                            Ok(block.into())
                        })
                        .await;

                    match block {
                        Err(err) => {
                            error!("starknet block poll error: {:?}", err);
                            return;
                        }
                        Ok(block) => {
                            trace!("got head: {} {}", block.number, block.hash);

                            let should_send = prev_block
                                .as_ref()
                                .map(|b| b.hash != block.hash)
                                .unwrap_or(true);

                            if should_send {
                                prev_block = Some(block.clone());
                                debug!("sending new head {} {}", block.number, block.hash);
                                if let Err(err) = tx.send(block.clone()).await {
                                    error!("starknet block poll error sending block: {:?}", err);
                                }
                            }

                            // wait to fetch block again
                            tokio::time::sleep(sleep_duration).await;
                        }
                    }
                }
            }
        });
        let stream = Box::pin(ReceiverStream::new(rx));
        Ok(stream)
    }
}

impl From<SNBlock> for BlockHeader {
    fn from(b: SNBlock) -> Self {
        let hash = BlockHash::from_bytes(&b.block_hash.to_bytes_be());
        let parent_hash = BlockHash::from_bytes(&b.parent_hash.to_bytes_be());
        BlockHeader {
            hash,
            parent_hash: Some(parent_hash),
            number: b.block_number,
            timestamp: b.accepted_time,
        }
    }
}

impl TryInto<FieldElement> for &BlockHash {
    type Error = Error;

    fn try_into(self) -> Result<FieldElement, Self::Error> {
        if self.as_bytes().len() == 32 {
            let mut buff: [u8; 32] = Default::default();
            buff.copy_from_slice(self.as_bytes());
            return Ok(FieldElement::from_bytes_be(&buff)?);
        }
        Err(Error::msg(""))
    }
}
