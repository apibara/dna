//! Track canonical chain.

use apibara_node::db::TableCursor;

pub struct CanonicalChain {}

impl CanonicalChain {
    pub fn new() -> Self {
        CanonicalChain {}
    }

    // pub fn latest_indexed_block(&self, cursor: TableCursor<'txn>)
}
