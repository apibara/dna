//! Aggregate data for one block.

use std::sync::Arc;

use apibara_node::db::libmdbx::{self, Environment, EnvironmentKind};

use crate::{
    core::{pb::starknet::v1alpha2, GlobalBlockId},
    db::StorageReader,
};

pub trait BlockDataAggregator {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a `Block` with data for the given block.
    ///
    /// If there is no data for the given block, it returns `None`.
    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error>;

    fn next_block(&self, block_id: &GlobalBlockId) -> Result<Option<GlobalBlockId>, Self::Error>;
}

pub struct DatabaseBlockDataAggregator<R: StorageReader> {
    storage: Arc<R>,
}

impl<R> DatabaseBlockDataAggregator<R>
where
    R: StorageReader,
{
    pub fn new(storage: Arc<R>) -> Self {
        DatabaseBlockDataAggregator { storage }
    }
}

impl<R> BlockDataAggregator for DatabaseBlockDataAggregator<R>
where
    R: StorageReader,
{
    type Error = R::Error;

    #[tracing::instrument(skip(self))]
    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error> {
        let status = self
            .storage
            .read_status(&block_id)?
            .unwrap_or(v1alpha2::BlockStatus::Unspecified);
        let header = self.storage.read_header(&block_id)?;
        let transactions = self.storage.read_body(&block_id)?;
        let receipts = self.storage.read_receipts(&block_id)?;
        let state_update = self.storage.read_state_update(&block_id)?;

        let block = v1alpha2::Block {
            status: status as i32,
            header,
            state_update,
            transactions,
            receipts,
        };

        Ok(Some(block))
    }

    #[tracing::instrument(skip(self))]
    fn next_block(&self, block_id: &GlobalBlockId) -> Result<Option<GlobalBlockId>, Self::Error> {
        self.storage.canonical_block_id(block_id.number() + 1)
    }
}
