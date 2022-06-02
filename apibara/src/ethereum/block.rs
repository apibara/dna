use std::sync::Arc;

use async_trait::async_trait;
use ethers::prelude::{Block, Middleware, PubsubClient, H256};
use futures::StreamExt;

use crate::block::{BlockHash, BlockHeader, BlockHeaderProvider, BlockHeaderStream};
use crate::error::{ApibaraError, Result};
use crate::ethereum::error::EthereumError;

pub struct EthereumBlockHeaderProvider<M: Middleware>
where
    M::Provider: PubsubClient,
{
    client: Arc<M>,
}

impl<M> EthereumBlockHeaderProvider<M>
where
    M: Middleware,
    M::Provider: PubsubClient,
{
    pub fn new(client: Arc<M>) -> Self {
        EthereumBlockHeaderProvider { client }
    }
}

#[async_trait]
impl<M> BlockHeaderProvider for EthereumBlockHeaderProvider<M>
where
    M: Middleware,
    M::Provider: PubsubClient,
{
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let block_number = self
            .client
            .get_block_number()
            .await
            .map_err(|_| ApibaraError::EthereumProviderError(EthereumError::ProviderError))?;
        let block = self
            .client
            .get_block(block_number)
            .await
            .map_err(|_| ApibaraError::EthereumProviderError(EthereumError::ProviderError))?
            .ok_or(ApibaraError::HeadBlockNotFound)?;

        ethers_block_to_block_header(block).map_err(ApibaraError::EthereumProviderError)
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let block = self
            .client
            .get_block(H256(hash.0))
            .await
            .map_err(|_| ApibaraError::EthereumProviderError(EthereumError::ProviderError))?;

        match block {
            None => Ok(None),
            Some(block) => ethers_block_to_block_header(block)
                .map(Some)
                .map_err(ApibaraError::EthereumProviderError),
        }
    }
    async fn subscribe_blocks<'a>(&'a self) -> Result<BlockHeaderStream<'a>> {
        let stream = self
            .client
            .subscribe_blocks()
            .await
            .map_err(|_| ApibaraError::EthereumProviderError(EthereumError::ProviderError))?;
        let transformed_stream =
            Box::pin(stream.map(|block| ethers_block_to_block_header(block).unwrap()));
        Ok(transformed_stream)
    }
}

fn ethers_block_to_block_header(
    block: Block<H256>,
) -> std::result::Result<BlockHeader, EthereumError> {
    let hash = block
        .hash
        .map(ethereum_block_hash)
        .ok_or(EthereumError::MissingBlockHash)?;

    let parent_hash = if block.parent_hash.is_zero() {
        None
    } else {
        Some(ethereum_block_hash(block.parent_hash))
    };

    let number = block
        .number
        .ok_or(EthereumError::MissingBlockNumber)?
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
