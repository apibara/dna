//! StarkNet provider.
use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use backoff::ExponentialBackoff;
use futures::Stream;
use starknet::core::types::FieldElement;
use starknet_rpc::{Block as SNBlock, BlockHash as SNBlockHash, Event as SNEvent, RpcProvider};
use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace};

use crate::chain::{
    Address, BlockEvents, BlockHash, BlockHeader, ChainProvider, Event, EventFilter, Topic,
    TopicValue,
};

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
                                // TODO: add backpressure
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

    async fn get_events_in_range(
        &self,
        from_block: u64,
        to_block: u64,
        filter: &EventFilter,
    ) -> Result<Vec<BlockEvents>> {
        let address: Option<FieldElement> =
            filter.address.as_ref().map(|a| a.try_into()).transpose()?;
        let mut topics: Vec<FieldElement> = Vec::new();
        for topic in &filter.topics {
            let topic = topic.try_into()?;
            topics.push(topic);
        }

        let mut block_events = BlockEventsBuilder::new();
        let mut page_number = 0;
        // TODO: need to keep events sorted. Need to have a block_index in the rpc
        // response.
        loop {
            let page = self
                .client
                .get_events(
                    Some(from_block),
                    Some(to_block),
                    address,
                    topics.clone(),
                    page_number,
                    100,
                )
                .await
                .context("failed to fetch starknet events")?;

            for event in &page.events {
                let block_hash = event.block_hash.into();
                block_events.add_event(event.block_number, block_hash, event.into());
            }

            page_number += 1;
            if page.is_last_page {
                break;
            }
        }

        block_events.build()
    }
}

#[derive(Debug, Default)]
struct BlockEventsBuilder {
    hashes: HashMap<u64, BlockHash>,
    events: HashMap<u64, Vec<Event>>,
}

impl BlockEventsBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add `event` to the events in that block.
    pub fn add_event(&mut self, block_number: u64, block_hash: BlockHash, event: Event) {
        let events_in_block = self.events.entry(block_number).or_default();
        events_in_block.push(event);
        self.hashes.entry(block_number).or_insert(block_hash);
    }

    /// Return a vector of `BlockEvents` with the accumulated events.
    pub fn build(self) -> Result<Vec<BlockEvents>> {
        let mut result = Vec::new();
        for (block_number, events) in self.events {
            let block_hash = self
                .hashes
                .get(&block_number)
                .ok_or_else(|| Error::msg("block missing hash"))?
                .to_owned();
            let block_with_events = BlockEvents {
                number: block_number,
                hash: block_hash,
                events,
            };
            result.push(block_with_events);
        }

        result.sort_by(|a, b| a.number.cmp(&b.number));

        Ok(result)
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

impl From<&SNEvent> for Event {
    fn from(e: &SNEvent) -> Self {
        let address = e.from_address.to_bytes_be().as_ref().into();
        let topics = e
            .keys
            .iter()
            .map(|t| t.to_bytes_be().as_ref().into())
            .collect();

        let data = e
            .data
            .iter()
            .map(|d| d.to_bytes_be().as_ref().into())
            .collect();

        // at the moment, starknet events don't have a block index.
        Event {
            address,
            topics,
            data,
            block_index: 0,
        }
    }
}

impl From<FieldElement> for BlockHash {
    fn from(fe: FieldElement) -> Self {
        BlockHash::from_bytes(&fe.to_bytes_be())
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

impl TryInto<FieldElement> for &Address {
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

impl TryInto<FieldElement> for &Topic {
    type Error = Error;

    fn try_into(self) -> Result<FieldElement, Self::Error> {
        match self {
            Topic::Value(ref value) => value.try_into(),
            Topic::Choice(_) => Err(Error::msg("choice not supported on starknet")),
        }
    }
}

impl TryInto<FieldElement> for &TopicValue {
    type Error = Error;

    fn try_into(self) -> Result<FieldElement, Self::Error> {
        let bytes = self.as_bytes();
        let bytes_len = bytes.len();
        if bytes_len <= 32 {
            let mut buff: [u8; 32] = Default::default();
            buff[32 - bytes_len..32].copy_from_slice(bytes);
            return Ok(FieldElement::from_bytes_be(&buff)?);
        }
        Err(Error::msg("invalid field element length"))
    }
}
