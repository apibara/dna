use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use ethers::prelude::{Block, Filter, Log, Middleware, PubsubClient, ValueOrArray, H160, H256};
use futures::{future, Future, StreamExt};
use std::{collections::HashMap, pin::Pin, sync::Arc};

use crate::chain::{
    Address, BlockEvents, BlockHash, BlockHeader, BlockHeaderStream, ChainProvider, Event,
    EventFilter, Topic, TopicValue,
};

pub struct EthereumChainProvider<M: Middleware + 'static>
where
    M::Provider: PubsubClient,
{
    client: Arc<M>,
}

impl<M> EthereumChainProvider<M>
where
    M: Middleware + 'static,
    M::Provider: PubsubClient,
{
    pub fn new(client: M) -> Self {
        EthereumChainProvider {
            client: Arc::new(client),
        }
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

    fn get_events_by_block_range(
        &self,
        from_block: u64,
        to_block: u64,
        filters: &[EventFilter],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BlockEvents>>> + Send>> {
        // convert here to avoid dealing with `filters` ref.
        let mut log_filters = Vec::new();
        for filter in filters {
            match to_ethers_filter(from_block, to_block, filter) {
                Ok(log_filter) => log_filters.push(log_filter),
                Err(err) => {
                    return Box::pin(future::ready(Err(err)));
                }
            }
        }
        Box::pin(get_all_events_by_block_range(
            self.client.clone(),
            from_block,
            to_block,
            log_filters,
        ))
    }

    async fn get_events_by_block_hash(&self, _hash: &BlockHash) -> Result<Vec<BlockEvents>> {
        todo!()
    }
}

async fn get_all_events_by_block_range<M>(
    client: Arc<M>,
    from_block: u64,
    to_block: u64,
    filters: Vec<Filter>,
) -> Result<Vec<BlockEvents>>
where
    M: Middleware,
    <M as Middleware>::Error: 'static,
{
    // perform each request in sequence.
    // expect not to be a performance issue because:
    //  - filters are few
    //  - time spent in user's code more than time fetching chain data
    let mut parsed_logs = Vec::new();
    for filter in &filters {
        let mut events = get_events_by_block_range(client.clone(), filter).await?;
        parsed_logs.append(&mut events)
    }

    let mut block_hashes: HashMap<u64, BlockHash> = HashMap::new();
    let mut block_events: HashMap<u64, Vec<Event>> = HashMap::new();

    for (block_number, block_hash, event) in parsed_logs {
        if !block_hashes.contains_key(&block_number) {
            block_hashes.insert(block_number, block_hash);
        }
        let events = block_events.entry(block_number).or_default();
        events.push(event);
    }

    let mut result = Vec::new();

    for block_number in from_block..to_block {
        let hash = block_hashes.remove(&block_number);
        let events = block_events.remove(&block_number);
        match (hash, events) {
            (Some(hash), Some(events)) => {
                let event = BlockEvents {
                    number: block_number,
                    hash,
                    events,
                };
                result.push(event);
            }
            _ => {
                // block did not contain any event
            }
        }
    }

    Ok(result)
}

async fn get_events_by_block_range<M>(
    client: Arc<M>,
    filter: &Filter,
) -> Result<Vec<(u64, BlockHash, Event)>>
where
    M: Middleware,
    <M as Middleware>::Error: 'static,
{
    let logs = client
        .get_logs(&filter)
        .await
        .context("failed to fetch eth logs")?;

    // parse and group by block
    let mut parsed_logs = Vec::new();
    for log in logs.into_iter().map(parse_ethereum_log) {
        match log {
            Err(err) => return Err(err),
            Ok(log) => parsed_logs.push(log),
        }
    }
    Ok(parsed_logs)
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

fn parse_ethereum_log(log: Log) -> Result<(u64, BlockHash, Event)> {
    let block_number = log
        .block_number
        .ok_or(Error::msg("missing eth log block number"))?
        .as_u64();
    let block_hash = log
        .block_hash
        .map(ethereum_block_hash)
        .ok_or(Error::msg("missing eth log block hash"))?;

    let address = ethereum_address(log.address);
    let data = log.data.to_vec();
    let topics = log
        .topics
        .iter()
        .map(|t| TopicValue(t.0.to_vec()))
        .collect();
    let log_index = log
        .log_index
        .ok_or(Error::msg("missing eth log index"))?
        .as_usize();

    let event = Event {
        address,
        topics,
        data,
        log_index,
    };

    Ok((block_number, block_hash, event))
}

fn ethereum_block_hash(hash: H256) -> BlockHash {
    BlockHash(hash.0)
}

fn ethereum_address(address: H160) -> Address {
    Address(address.0.to_vec())
}

fn to_ethers_filter(from_block: u64, to_block: u64, event_filter: &EventFilter) -> Result<Filter> {
    let filter = Filter::new().from_block(from_block).to_block(to_block);

    let filter = if event_filter.address.len() > 0 {
        let mut address_filter = Vec::new();
        for address in &event_filter.address {
            let address = address_to_ethers(address)?;
            address_filter.push(address);
        }
        filter.address(ValueOrArray::Array(address_filter))
    } else {
        filter
    };

    let mut topic_iter = event_filter.topics.iter();

    let filter = if let Some(topic) = topic_iter.next() {
        let topic = topic_to_ethers(topic)?;
        filter.topic0(topic)
    } else {
        return Ok(filter);
    };

    let filter = if let Some(topic) = topic_iter.next() {
        let topic = topic_to_ethers(topic)?;
        filter.topic1(topic)
    } else {
        return Ok(filter);
    };

    let filter = if let Some(topic) = topic_iter.next() {
        let topic = topic_to_ethers(topic)?;
        filter.topic2(topic)
    } else {
        return Ok(filter);
    };

    let filter = if let Some(topic) = topic_iter.next() {
        let topic = topic_to_ethers(topic)?;
        filter.topic3(topic)
    } else {
        return Ok(filter);
    };

    if let Some(_) = topic_iter.next() {
        return Err(Error::msg("invalid number of eth topics"));
    }

    Ok(filter)
}

fn address_to_ethers(address: &Address) -> Result<H160> {
    if address.0.len() == 20 {
        Ok(H160::from_slice(&address.0))
    } else {
        Err(Error::msg("eth address is expected to be 20 bytes"))
    }
}

fn topic_to_ethers(topic: &Topic) -> Result<ValueOrArray<H256>> {
    match topic {
        Topic::Value(topic) => Ok(ValueOrArray::Value(topic_value_to_ethers(topic)?)),
        Topic::Choice(choice) => {
            let mut values = Vec::new();
            for topic in choice {
                let value = topic_value_to_ethers(topic)?;
                values.push(value);
            }
            Ok(ValueOrArray::Array(values))
        }
    }
}

fn topic_value_to_ethers(topic: &TopicValue) -> Result<H256> {
    if topic.0.len() == 32 {
        Ok(H256::from_slice(&topic.0))
    } else {
        Err(Error::msg("eth topic is expected to be 32 bytes"))
    }
}
