//! Types common to all chains.
use chrono::NaiveDateTime;
use std::fmt;

/// Chain block hash.
#[derive(Clone, PartialEq)]
pub struct BlockHash(pub(crate) Vec<u8>);

/// Chain address.
#[derive(Clone)]
pub struct Address(pub(crate) Vec<u8>);

/// Block header information needed to track information about the chain head.
#[derive(Debug, Clone)]
pub struct BlockHeader {
    /// This block's hash.
    pub hash: BlockHash,
    /// The block parent's hash.
    pub parent_hash: Option<BlockHash>,
    /// This block height.
    pub number: u64,
    /// Time the block was finalized.
    pub timestamp: NaiveDateTime,
}

/// Data associated with an event.
#[derive(Clone)]
pub struct TopicValue(pub(crate) Vec<u8>);

/// A blockchain event.
#[derive(Debug)]
pub struct Event {
    /// Event source address.
    pub address: Address,
    /// Indexed events.
    pub topics: Vec<TopicValue>,
    /// Event data.
    pub data: Vec<u8>,
    /// Event index in the block.
    pub block_index: usize,
}

/// Events emitted in a single block.
#[derive(Debug)]
pub struct BlockEvents {
    /// The block number.
    pub number: u64,
    /// The block hash.
    pub hash: BlockHash,
    /// The events.
    pub events: Vec<Event>,
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
    hex::encode(&h.0)
}

fn topic_value_to_hex(t: &TopicValue) -> String {
    hex::encode(&t.0)
}

fn address_to_hex(a: &Address) -> String {
    hex::encode(&a.0)
}