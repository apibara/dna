//! Transaction data.

use apibara_node::db::Table;

use super::block::{BlockBody, BlockEvents, BlockReceipts, ContractAtBlockId};
use crate::core::GlobalBlockId;

/// Store block body.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockBodyTable {}

/// Store block receipts.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockReceiptsTable {}

/// Store block events.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockEventsTable {}

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

impl Table for BlockEventsTable {
    type Key = ContractAtBlockId;
    type Value = BlockEvents;

    fn db_name() -> &'static str {
        "BlockEvents"
    }
}
