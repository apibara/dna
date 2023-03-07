//! Transaction data.

use apibara_node::db::Table;

use super::block::{BlockBody, BlockReceipts};
use crate::core::GlobalBlockId;

/// Store block body.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockBodyTable {}

/// Store block receipts.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockReceiptsTable {}

impl Table for BlockBodyTable {
    type Key = GlobalBlockId;
    type Value = BlockBody;

    fn db_name() -> &'static str {
        "BlockBody"
    }
}

impl Table for BlockReceiptsTable {
    type Key = GlobalBlockId;
    type Value = BlockReceipts;

    fn db_name() -> &'static str {
        "BlockReceipts"
    }
}
