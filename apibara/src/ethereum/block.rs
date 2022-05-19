use std::sync::Arc;

use async_trait::async_trait;
use ethers::prelude::{Block, Middleware, H256};

use crate::block::{BlockHeader, BlockHeaderProvider};
use crate::error::{ApibaraError, Result};
use crate::ethereum::error::EthereumError;

#[derive(Debug)]
pub struct EthereumBlockHash(pub(crate) [u8; 32]);

pub struct EthereumBlockHeaderProvider<M: Middleware> {
    client: Arc<M>,
}

impl<M: Middleware> EthereumBlockHeaderProvider<M> {
    pub fn new(client: Arc<M>) -> Self {
        EthereumBlockHeaderProvider { client }
    }
}

#[async_trait]
impl<M: Middleware> BlockHeaderProvider for EthereumBlockHeaderProvider<M> {
    type BlockHash = EthereumBlockHash;

    async fn get_head_block(&self) -> Result<BlockHeader<Self::BlockHash>> {
        let block_number = self.client.get_block_number().await.map_err(|_| {
            ApibaraError::EthereumProviderError(EthereumError::ProviderError)
        })?;
        self.get_block_by_number(block_number.as_u64())
            .await?
            .ok_or(ApibaraError::HeadBlockNotFound)
    }

    async fn get_block_by_hash(
        &self,
        hash: &Self::BlockHash,
    ) -> Result<Option<BlockHeader<Self::BlockHash>>> {
        let block = self.client.get_block(H256(hash.0)).await.map_err(|_| {
            ApibaraError::EthereumProviderError(EthereumError::ProviderError)
        })?;

        match block {
            None => Ok(None),
            Some(block) => ethers_block_to_block_header(block)
                .map(Some)
                .map_err(ApibaraError::EthereumProviderError),
        }
    }

    async fn get_block_by_number(
        &self,
        number: u64,
    ) -> Result<Option<BlockHeader<Self::BlockHash>>> {
        let block = self.client.get_block(number).await.map_err(|_| {
            ApibaraError::EthereumProviderError(EthereumError::ProviderError)
        })?;

        match block {
            None => Ok(None),
            Some(block) => ethers_block_to_block_header(block)
                .map(Some)
                .map_err(ApibaraError::EthereumProviderError),
        }
    }
}

fn ethers_block_to_block_header(
    block: Block<H256>,
) -> std::result::Result<BlockHeader<EthereumBlockHash>, EthereumError> {
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

fn ethereum_block_hash(hash: H256) -> EthereumBlockHash {
    EthereumBlockHash(hash.0)
}
