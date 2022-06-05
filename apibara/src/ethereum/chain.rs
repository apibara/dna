use async_trait::async_trait;
use anyhow::{Context, Error, Result};
use ethers::prelude::{Block, Middleware, PubsubClient, H256};
use futures::StreamExt;

use crate::chain::{BlockHash, BlockHeader, ChainProvider, BlockHeaderStream};

pub struct EthereumChainProvider<M: Middleware>
where
    M::Provider: PubsubClient,
{
    client: M,
}

impl<M> EthereumChainProvider<M>
where
    M: Middleware,
    M::Provider: PubsubClient,
{
    pub fn new(client: M) -> Self {
        EthereumChainProvider { client }
    }
}

#[async_trait]
impl<M> ChainProvider for EthereumChainProvider<M>
where
    M: Middleware,
    M::Provider: PubsubClient,
    <M as Middleware>::Error: 'static
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
            Some(block) => ethers_block_to_block_header(block)
                .map(Some)
        }
    }
    async fn subscribe_blocks<'a>(&'a self) -> Result<BlockHeaderStream<'a>> {
        let stream = self
            .client
            .subscribe_blocks()
            .await
            .context("failed to subscribe eth blocks")?;
        let transformed_stream =
            Box::pin(stream.map(|block| ethers_block_to_block_header(block).unwrap()));
        Ok(transformed_stream)
    }
}

fn ethers_block_to_block_header(
    block: Block<H256>,
) -> Result<BlockHeader> {
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
