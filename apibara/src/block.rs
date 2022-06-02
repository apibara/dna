use super::error::Result;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::stream::BoxStream;
use std::fmt;

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

pub type BlockHeaderStream<'a> = BoxStream<'a, BlockHeader>;

#[async_trait]
pub trait BlockHeaderProvider {
    /// Get the most recent (head) block.
    async fn get_head_block(&self) -> Result<BlockHeader>;

    /// Get a specific block by its hash.
    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>>;

    async fn subscribe_blocks<'a>(&'a self) -> Result<BlockHeaderStream<'a>>;
}

fn hash_to_hex(h: &BlockHash) -> String {
    hex::encode(h.0.as_ref())
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
