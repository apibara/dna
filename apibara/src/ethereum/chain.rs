use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use ethers::{
    prelude::{Block, Middleware, PubsubClient, H256},
    utils::WEI_IN_ETHER,
};
use futures::{future, Future, Stream, StreamExt};
use std::{
    pin::Pin,
    task::{self, Poll},
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::chain::{BlockEvents, BlockHash, BlockHeader, BlockHeaderStream, ChainProvider};

pub struct EthereumChainProvider<M: Middleware + 'static>
where
    M::Provider: PubsubClient,
{
    client: M,
}

impl<M> EthereumChainProvider<M>
where
    M: Middleware + 'static,
    M::Provider: PubsubClient,
{
    pub fn new(client: M) -> Self {
        EthereumChainProvider { client }
    }
}

#[async_trait]
impl<M> ChainProvider for EthereumChainProvider<M>
where
    M: Middleware + Clone + 'static,
    M::Provider: PubsubClient,
    <M as Middleware>::Error: 'static,
{
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let block_number = self
            .client
            .get_block_number()
            .await
            .context("failed to get eth block number")?;
        let block = self
            .client
            .get_block(block_number)
            .await
            .context("failed to get eth block by number")?
            .ok_or(Error::msg("block does not exist"))?;

        ethers_block_to_block_header(block)
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let block = self
            .client
            .get_block(H256(hash.0))
            .await
            .context("failed to get eth block by hash")?;

        match block {
            None => Ok(None),
            Some(block) => ethers_block_to_block_header(block).map(Some),
        }
    }

    async fn blocks_subscription(&self) -> Result<BlockHeaderStream> {
        let original_stream = self
            .client
            .subscribe_blocks()
            .await
            .context("failed to subscribe eth blocks")?;

        let transformed_stream = original_stream
            .map(ethers_block_to_block_header)
            .take_while(|r| future::ready(Result::is_ok(r)))
            .map(|r| r.unwrap()); // safe because we stop while all ok
        Ok(Box::pin(transformed_stream))
    }

    async fn get_events_by_block_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<BlockEvents> {
        todo!()
    }

    async fn get_events_by_block_hash(&self, hash: &BlockHash) -> Result<BlockEvents> {
        todo!()
    }
}

fn ethers_block_to_block_header(block: Block<H256>) -> Result<BlockHeader> {
    let hash = block
        .hash
        .map(ethereum_block_hash)
        .ok_or(Error::msg("missing block hash"))?;

    let parent_hash = if block.parent_hash.is_zero() {
        None
    } else {
        Some(ethereum_block_hash(block.parent_hash))
    };

    let number = block
        .number
        .ok_or(Error::msg("missing block number"))?
        .as_u64();

    let timestamp = chrono::NaiveDateTime::from_timestamp(block.timestamp.as_u32() as i64, 0);

    Ok(BlockHeader {
        hash,
        parent_hash,
        number,
        timestamp,
    })
}

fn ethereum_block_hash(hash: H256) -> BlockHash {
    BlockHash(hash.0)
}
