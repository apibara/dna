//! Abstraction over chain providers.
//!
//! Chain providers are used to fetch and subscribe to
//! blockchain data.
use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use std::pin::Pin;

use crate::chain::types::{BlockHash, BlockHeader};

/// Provide information about blocks and events/logs on a chain.
#[async_trait]
pub trait ChainProvider: Send + Sync + 'static {
    /// Get the most recent (head) block.
    async fn get_head_block(&self) -> Result<BlockHeader>;

    /// Get a specific block by its hash.
    async fn get_block_by_hash(&self, hash: &BlockHash) -> Result<Option<BlockHeader>>;

    fn subscribe_blocks(&self) -> Result<Pin<Box<dyn Stream<Item = BlockHeader> + Send>>>;
}
