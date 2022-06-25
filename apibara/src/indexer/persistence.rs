//! Store indexer state between restarts.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{chain::EventFilter, persistence::Id};

/// Indexer state.
#[derive(Debug, Serialize, Deserialize)]
pub struct State {
    /// Unique id.
    pub id: Id,
    /// Event filters.
    pub filters: Vec<EventFilter>,
    /// Number of blocks to fetch in one call.
    pub block_batch_size: usize,
    /// Starting block.
    pub index_from_block: u64,
    /// Most recent indexed block.
    pub indexed_to_block: Option<u64>,
}

/// Persist indexer state to storage.
#[async_trait]
pub trait IndexerPersistence: Send + Sync + 'static {
    /// Get the specified indexer, if any.
    async fn get_indexer(&self, id: &Id) -> Result<Option<State>>;

    /// Create a new indexer. Returns error if the id is not unique.
    async fn create_indexer(&self, state: &State) -> Result<()>;

    /// Delete the specified indexer.
    async fn delete_indexer(&self, id: &Id) -> Result<()>;

    /// List all indexers.
    async fn list_indexer(&self) -> Result<Vec<State>>;

    /// Update the specified indexer's `indexed_to_block` to the given block.
    ///
    /// Given how often the indexer's indexed block is updated, we require
    /// a specialized method that databases can leverage to implement a more
    /// efficient storage update.
    async fn update_indexer_block(&self, id: &Id, new_indexed_block: u64) -> Result<()>;
}
