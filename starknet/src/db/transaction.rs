//! Transaction data.

use apibara_node::db::Table;
use prost::Message;

use crate::core::{pb::v1alpha2, GlobalBlockId};

/// Store block body.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockBodyTable {}

#[derive(Clone, PartialEq, Message)]
pub struct BlockBody {
    #[prost(message, repeated, tag = "1")]
    pub transactions: prost::alloc::vec::Vec<v1alpha2::Transaction>,
}

/// Store block receipts.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockReceiptsTable {}

#[derive(Clone, PartialEq, Message)]
pub struct BlockReceipts {
    #[prost(message, repeated, tag = "1")]
    pub receipts: prost::alloc::vec::Vec<v1alpha2::TransactionReceipt>,
}

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
