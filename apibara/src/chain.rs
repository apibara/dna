//! Blockchain-related traits and types.
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::stream::BoxStream;
use futures::{Future, Stream};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Chain block hash.
#[derive(Clone, PartialEq)]
pub struct BlockHash(pub(crate) [u8; 32]);

/// Block header information needed to track information about the chain head.
#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub hash: BlockHash,
    pub parent_hash: Option<BlockHash>,
    pub number: u64,
    pub timestamp: NaiveDateTime,
}

#[derive(Debug)]
pub struct Event {}

#[derive(Debug)]
pub struct BlockEvents {
    pub number: u64,
    pub hash: BlockHash,
    pub events: Vec<Event>,
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
    async fn get_events_by_block_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<BlockEvents>;

    /// Get events in the specified block.
    async fn get_events_by_block_hash(&self, hash: &BlockHash) -> Result<BlockEvents>;
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

fn hash_to_hex(h: &BlockHash) -> String {
    hex::encode(h.0.as_ref())
}
