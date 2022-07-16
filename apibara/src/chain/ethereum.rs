//! Ethereum (and Ethereum-like) provider.
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use ethers::prelude::{
    Block, BlockNumber, Filter, Log, Middleware, Provider, SubscriptionStream, ValueOrArray, Ws,
    H160, H256, U256,
};
use futures::{Stream, StreamExt};
use tracing::error;

use super::{
    block_events::BlockEventsBuilder, types::EventsWithBlockNumberHash, Address, BlockHash,
    BlockHeader, ChainProvider, EthereumEvent, Event, EventFilter, TopicValue,
};

#[derive(Debug)]
pub struct EthereumProvider {
    client: Arc<Provider<Ws>>,
}

impl EthereumProvider {
    pub async fn new(url: &str) -> Result<EthereumProvider> {
        let client = Provider::<Ws>::connect(url)
            .await
            .context("failed to connect to ethereum provider")?;

        Ok(EthereumProvider {
            client: Arc::new(client),
        })
    }

    async fn accumulate_filter_events(
        &self,
        block_events: &mut BlockEventsBuilder,
        from_block: u64,
        to_block: u64,
        filter: &EventFilter,
    ) -> Result<()> {
        let ethers_filter = Filter::new().from_block(from_block).to_block(to_block);

        let ethers_filter = match filter.address {
            None => ethers_filter,
            Some(ref address) => {
                let ethers_addr: H160 = address.clone().try_into()?;
                ethers_filter.address(ValueOrArray::Value(ethers_addr))
            }
        };

        // TODO: add topics

        let logs = self
            .client
            .get_logs(&ethers_filter)
            .await
            .context("failed to fetch ethereum logs")?;

        for log in logs {
            let block_hash: BlockHash = log
                .block_hash
                .ok_or_else(|| Error::msg("missing log block hash"))?
                .into();
            let block_number = log
                .block_number
                .ok_or_else(|| Error::msg("missing log block number"))?
                .as_u64();

            let event: EthereumEvent = log.into();
            block_events.add_event(block_number, block_hash, Event::Ethereum(event));
        }

        Ok(())
    }
}

#[async_trait]
impl ChainProvider for EthereumProvider {
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let block = self
            .client
            .get_block(BlockNumber::Latest)
            .await
            .context("failed to fetch latest ethereum block")?
            .ok_or_else(|| Error::msg("expected latest block to exist"))?;
        block.try_into()
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let hash: H256 = hash.try_into()?;
        let block = self
            .client
            .get_block(hash)
            .await
            .context("failed to fetch ethereum block by hash")?;
        block.map(TryInto::try_into).transpose()
    }

    async fn subscribe_blocks<'a>(
        &'a self,
    ) -> Result<Pin<Box<dyn Stream<Item = BlockHeader> + Send + 'a>>> {
        let inner = self
            .client
            .subscribe_blocks()
            .await
            .context("failed to start streaming ethereum blocks")?;
        let stream = Box::pin(BlocksStream { inner });
        Ok(stream)
    }

    async fn get_events_in_range(
        &self,
        from_block: u64,
        to_block: u64,
        filters: &[EventFilter],
    ) -> Result<Vec<EventsWithBlockNumberHash>> {
        let mut block_events = BlockEventsBuilder::new();
        for filter in filters {
            self.accumulate_filter_events(&mut block_events, from_block, to_block, filter)
                .await?;
        }
        block_events.build()
    }
}

struct BlocksStream<'a> {
    inner: SubscriptionStream<'a, Ws, Block<H256>>,
}

impl<'a> Stream for BlocksStream<'a> {
    type Item = BlockHeader;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(block)) => match block.try_into() {
                Ok(block) => Poll::Ready(Some(block)),
                Err(err) => {
                    error!("failed to parse streaming block: {:?}", err);
                    Poll::Ready(None)
                }
            },
        }
    }
}

impl From<H256> for BlockHash {
    fn from(h: H256) -> Self {
        BlockHash::from_bytes(h.as_bytes())
    }
}

impl TryInto<H256> for &BlockHash {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<H256, Self::Error> {
        let bytes = self.as_bytes();
        if bytes.len() != 32 {
            return Err(Error::msg("invalid hash byte size"));
        }
        let mut buf = [0u8; 32];
        buf.copy_from_slice(bytes);
        Ok(H256(buf))
    }
}

impl TryFrom<Address> for H160 {
    type Error = anyhow::Error;

    fn try_from(addr: Address) -> Result<Self, Self::Error> {
        if addr.as_bytes().len() != 20 {
            return Err(Error::msg("invalid ethereum address size"));
        }
        let mut buf = [0u8; 20];
        buf.copy_from_slice(addr.as_bytes());
        Ok(H160(buf))
    }
}

struct U256Timestamp(pub U256);

impl From<U256Timestamp> for NaiveDateTime {
    fn from(timestamp: U256Timestamp) -> Self {
        NaiveDateTime::from_timestamp(timestamp.0.as_u64() as i64, 0)
    }
}

impl TryFrom<Block<H256>> for BlockHeader {
    type Error = anyhow::Error;

    fn try_from(value: Block<H256>) -> Result<Self, Self::Error> {
        let hash: BlockHash = value
            .hash
            .map(Into::into)
            .ok_or_else(|| Error::msg("missing block hash"))?;
        let parent_hash: BlockHash = value.parent_hash.into();
        let timestamp: NaiveDateTime = U256Timestamp(value.timestamp).into();
        let number = value
            .number
            .map(|n| n.as_u64())
            .ok_or_else(|| Error::msg("missing block number"))?;

        Ok(BlockHeader {
            hash,
            parent_hash: Some(parent_hash),
            timestamp,
            number,
        })
    }
}

impl From<Log> for EthereumEvent {
    fn from(log: Log) -> Self {
        let address: Address = log.address.as_bytes().into();
        let topics: Vec<TopicValue> = log.topics.iter().map(|t| t.as_bytes().into()).collect();
        let data = log.data.0.to_vec();
        let log_index = log.log_index.unwrap_or_default().as_usize();

        EthereumEvent {
            address,
            topics,
            data,
            log_index,
        }
    }
}
