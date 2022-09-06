//! StarkNet provider.
use std::{
    convert::{TryFrom, TryInto},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Error, Result};
use async_trait::async_trait;
use backoff::ExponentialBackoff;
use chrono::NaiveDateTime;
use futures::Stream;
use starknet::{
    core::{types::FieldElement, utils::get_selector_from_name},
    providers::jsonrpc::{
        models::{
            BlockId, BlockTag, EmittedEvent as SNEmittedEvent, EventFilter as SNEventFilter,
            MaybePendingBlockWithTxHashes as SNBlock,
        },
        HttpTransport, JsonRpcClient,
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, trace};
use url::Url;

use crate::chain::{
    Address, BlockHash, BlockHeader, ChainProvider, Event, EventFilter, StarkNetEvent, TopicValue,
};

use super::{block_events::BlockEventsBuilder, types::EventsWithBlockNumberHash};

#[derive(Debug)]
pub struct StarkNetProvider {
    client: Arc<JsonRpcClient<HttpTransport>>,
}

impl StarkNetProvider {
    pub fn new(url: &str) -> Result<StarkNetProvider> {
        let url: Url = url.try_into()?;
        let transport = HttpTransport::new(url);
        let client = JsonRpcClient::new(transport);
        Ok(StarkNetProvider {
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
        let address: Option<FieldElement> =
            filter.address.as_ref().map(|a| a.try_into()).transpose()?;

        let topics = if filter.signature.is_empty() {
            None
        } else {
            let event_selector = get_selector_from_name(&filter.signature)
                .map_err(|_| Error::msg("invalid ethereum event name"))?;
            Some(vec![event_selector])
        };

        let mut page_number = 0;
        // TODO: need to keep events sorted. Need to have a block_index in the rpc
        // response.
        // use a fake block index to keep events of the same type sorted
        let mut fake_block_index = 0;
        let event_filter = SNEventFilter {
            from_block: Some(BlockId::Number(from_block)),
            to_block: Some(BlockId::Number(to_block)),
            address,
            keys: topics,
        };

        loop {
            let page = self
                .client
                .get_events(event_filter.clone(), 100, page_number)
                .await
                .context("failed to fetch starknet events")?;

            for event in &page.events {
                let block_hash = event.block_hash.into();
                let mut new_event: StarkNetEvent = event.into();
                new_event.log_index = fake_block_index;
                fake_block_index += 1;
                block_events.add_event(event.block_number, block_hash, Event::StarkNet(new_event));
            }

            page_number += 1;
            if page.is_last_page {
                break;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ChainProvider for StarkNetProvider {
    async fn get_head_block(&self) -> Result<BlockHeader> {
        let tag = BlockId::Tag(BlockTag::Latest);
        let block = self
            .client
            .get_block_with_tx_hashes(&tag)
            .await
            .context("failed to fetch starknet head")?;
        block.try_into()
    }

    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>> {
        let hash_fe = hash
            .try_into()
            .context("failed to convert block hash to field element")?;
        let hash = BlockId::Hash(hash_fe);
        let block = self
            .client
            .get_block_with_tx_hashes(&hash)
            .await
            .context("failed to fetch starknet head")?;
        block.try_into().map(Option::Some)
    }

    async fn subscribe_blocks<'a>(
        &'a self,
    ) -> Result<Pin<Box<dyn Stream<Item = BlockHeader> + Send + 'a>>> {
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
                            let tag = BlockId::Tag(BlockTag::Latest);
                            let block = client
                                .get_block_with_tx_hashes(&tag)
                                .await
                                .context("failed to fetch starknet head")?;
                            block.try_into().map_err(Into::into)
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
                                // no backpressure needed since it's live block which are not produced
                                // often enough to create issues downstream.
                                trace!("sending new head {} {}", block.number, block.hash);
                                if let Err(err) = tx.send(block.clone()).await {
                                    error!("starknet block poll error sending block: {:?}", err);
                                    return;
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

impl TryFrom<SNBlock> for BlockHeader {
    type Error = anyhow::Error;

    fn try_from(b: SNBlock) -> Result<Self, Self::Error> {
        let header = match b {
            SNBlock::Block(block) => block.header,
            SNBlock::PendingBlock(_) => return Err(anyhow!("Block is still pending")),
        };

        let hash = BlockHash::from_bytes(&header.block_hash.to_bytes_be());
        let parent_hash = BlockHash::from_bytes(&header.parent_hash.to_bytes_be());
        let timestamp = NaiveDateTime::from_timestamp(header.timestamp as i64, 0);

        Ok(BlockHeader {
            hash,
            parent_hash: Some(parent_hash),
            timestamp,
            number: header.block_number,
        })
    }
}

impl From<&SNEmittedEvent> for StarkNetEvent {
    fn from(e: &SNEmittedEvent) -> Self {
        let address = e.event.from_address.to_bytes_be().as_ref().into();
        let transaction_hash = e.transaction_hash.to_bytes_be().as_ref().into();
        let topics = e
            .event
            .content
            .keys
            .iter()
            .map(|t| t.to_bytes_be().as_ref().into())
            .collect();

        let data = e
            .event
            .content
            .data
            .iter()
            .map(|d| d.to_bytes_be().as_ref().into())
            .collect();

        // at the moment, starknet events don't have a block index.
        StarkNetEvent {
            address,
            topics,
            data,
            transaction_hash,
            log_index: 0,
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
