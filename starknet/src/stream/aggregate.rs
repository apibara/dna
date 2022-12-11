//! Aggregate data for one block.

use std::sync::Arc;

use apibara_node::db::libmdbx::{self, Environment, EnvironmentKind};

use crate::core::{pb::v1alpha2, GlobalBlockId};

pub trait BlockDataAggregator {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a `Block` with data for the given block.
    ///
    /// If there is no data for the given block, it returns `None`.
    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error>;
}

pub struct DatabaseBlockDataAggregator<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    filter: v1alpha2::Filter,
}

impl<E> DatabaseBlockDataAggregator<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, filter: v1alpha2::Filter) -> Self {
        DatabaseBlockDataAggregator { db, filter }
    }
}

impl<E> BlockDataAggregator for DatabaseBlockDataAggregator<E>
where
    E: EnvironmentKind,
{
    type Error = libmdbx::Error;

    fn aggregate_for_block(
        &self,
        block_id: &GlobalBlockId,
    ) -> Result<Option<v1alpha2::Block>, Self::Error> {
        todo!()
    }
}
