//! State update data.

use apibara_core::starknet::v1alpha2;
use apibara_node::db::Table;

use crate::core::GlobalBlockId;

use super::block::ContractAtBlockId;

/// Store state updates without storage diffs.
#[derive(Debug, Clone, Copy, Default)]
pub struct StateUpdateTable {}

/// Store storage diffs for a given block.
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageDiffTable {}

impl Table for StateUpdateTable {
    type Key = GlobalBlockId;
    type Value = v1alpha2::StateUpdate;

    fn db_name() -> &'static str {
        "StateUpdate"
    }
}

impl Table for StorageDiffTable {
    type Key = ContractAtBlockId;
    type Value = v1alpha2::StorageDiff;

    fn db_name() -> &'static str {
        "StorageDiff"
    }
}
