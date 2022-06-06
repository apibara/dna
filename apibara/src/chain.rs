//! Blockchain-related traits and types.
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::{stream::BoxStream, Future};
use std::{fmt, pin::Pin};

/// Chain block hash.
#[derive(Clone, PartialEq)]
pub struct BlockHash(pub(crate) [u8; 32]);

/// Chain address.
#[derive(Clone)]
pub struct Address(pub(crate) Vec<u8>);

/// Block header information needed to track information about the chain head.
#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub hash: BlockHash,
    pub parent_hash: Option<BlockHash>,
    pub number: u64,
    pub timestamp: NaiveDateTime,
}

#[derive(Debug)]
pub struct Event {
    pub address: Address,
    pub topics: Vec<TopicValue>,
    pub data: Vec<u8>,
    pub log_index: usize,
}

#[derive(Debug)]
pub struct BlockEvents {
    pub number: u64,
    pub hash: BlockHash,
    pub events: Vec<Event>,
}

#[derive(Clone)]
pub struct TopicValue(pub(crate) Vec<u8>);

/// An event topic.
#[derive(Debug, Clone)]
pub enum Topic {
    Value(TopicValue),
    Choice(Vec<TopicValue>),
}

/// Describe how to filter events.
#[derive(Debug, Clone)]
pub struct EventFilter {
    /// Filter by the contract emitting the event.
    pub address: Vec<Address>,
    /// Filter by topics.
    pub topics: Vec<Topic>,
}

pub type BlockHeaderStream<'a> = BoxStream<'a, BlockHeader>;

/// Provide information about blocks and events/logs on a chain.
#[async_trait]
pub trait ChainProvider {
    /// Get the most recent (head) block.
    async fn get_head_block(&self) -> Result<BlockHeader>;

    /// Get a specific block by its hash.
    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>>;

    /// Subscribe to new blocks.
    async fn blocks_subscription(&self) -> Result<BlockHeaderStream>;

    /// Get events in blocks from `from_range` to `to_range`, inclusive.
    fn get_events_by_block_range(
        &self,
        from_block: u64,
        to_block: u64,
        filters: &[EventFilter],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<BlockEvents>>> + Send>>;

    /// Get events in the specified block.
    async fn get_events_by_block_hash(&self, hash: &BlockHash) -> Result<Vec<BlockEvents>>;
}

impl EventFilter {
    pub fn new() -> EventFilter {
        EventFilter {
            address: Vec::new(),
            topics: Vec::new(),
        }
    }

    pub fn add_address(mut self, address: Address) -> Self {
        self.address.push(address);
        self
    }

    pub fn add_topic(mut self, topic: Topic) -> Self {
        self.topics.push(topic);
        self
    }
}

impl Topic {
    pub fn value_from_hex(data: &str) -> Result<Topic> {
        let slice = if data.starts_with("0x") {
            &data[2..]
        } else {
            &data
        };
        let bytes = hex::decode(slice).context("failed to parse topic from hex")?;
        let value = TopicValue(bytes);
        Ok(Topic::Value(value))
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hash_to_hex(self))
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockHash({})", self)
    }
}

impl fmt::Display for TopicValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", topic_value_to_hex(self))
    }
}

impl fmt::Debug for TopicValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TopicValue({})", self)
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", address_to_hex(self))
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Address({})", self)
    }
}

fn hash_to_hex(h: &BlockHash) -> String {
    hex::encode(h.0.as_ref())
}

fn topic_value_to_hex(t: &TopicValue) -> String {
    hex::encode(&t.0)
}

fn address_to_hex(a: &Address) -> String {
    hex::encode(&a.0)
}
