//! Canonical chain.

use crate::core::pb::v1alpha2;
use apibara_node::db::Table;

/// Store canonical chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct CanonicalChainTable {}

impl Table for CanonicalChainTable {
    type Key = u64;
    type Value = v1alpha2::BlockHash;

    fn db_name() -> &'static str {
        "CanonicalChain"
    }
}
