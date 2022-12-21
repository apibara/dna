//! Transaction data.

use apibara_node::db::Table;

use crate::core::{pb::starknet::v1alpha2, GlobalBlockId};

/// Store block body.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockBodyTable {}

/// Store block receipts.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockReceiptsTable {}

impl Table for BlockBodyTable {
    type Key = GlobalBlockId;
    type Value = v1alpha2::BlockBody;

    fn db_name() -> &'static str {
        "BlockBody"
    }
}

impl Table for BlockReceiptsTable {
    type Key = GlobalBlockId;
    type Value = v1alpha2::BlockReceipts;

    fn db_name() -> &'static str {
        "BlockReceipts"
    }
}
