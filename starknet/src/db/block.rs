//! Block data.

use std::io::Cursor;

use apibara_core::starknet::v1alpha2;
use apibara_node::db::{KeyDecodeError, Table, TableKey};
use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

use crate::core::{BlockHash, GlobalBlockId};

pub struct ContractAtBlockId {
    pub block_id: GlobalBlockId,
    pub contract_address: v1alpha2::FieldElement,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockBody {
    #[prost(message, repeated, tag = "1")]
    pub transactions: prost::alloc::vec::Vec<v1alpha2::Transaction>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockReceipts {
    #[prost(message, repeated, tag = "1")]
    pub receipts: prost::alloc::vec::Vec<v1alpha2::TransactionReceipt>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockEvents {
    #[prost(message, repeated, tag = "1")]
    pub events: prost::alloc::vec::Vec<v1alpha2::Event>,
}

/// Store block status.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockStatusTable {}

#[derive(Clone, PartialEq, Message)]
pub struct BlockStatus {
    #[prost(enumeration = "v1alpha2::BlockStatus", tag = "1")]
    pub status: i32,
}

/// Store block header.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockHeaderTable {}

impl TableKey for BlockHash {
    type Encoded = [u8; 32];

    fn encode(&self) -> Self::Encoded {
        (*self).into_bytes()
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        BlockHash::from_slice(b).map_err(|err| KeyDecodeError::InvalidByteSize {
            expected: err.expected,
            actual: err.actual,
        })
    }
}

// A pair (block number, block hash) is encoded as:
// - 8 bytes big endian representation of the block number
// - 32 bytes block hash
impl TableKey for GlobalBlockId {
    type Encoded = [u8; 40];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 40];
        out[..8].copy_from_slice(&self.number().to_be_bytes());
        out[8..].copy_from_slice(self.hash().as_bytes());
        out
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        let block_hash =
            BlockHash::from_slice(&b[8..]).map_err(|err| KeyDecodeError::InvalidByteSize {
                expected: err.expected,
                actual: err.actual,
            })?;
        Ok(GlobalBlockId::new(block_number, block_hash))
    }
}

impl Table for BlockStatusTable {
    type Key = GlobalBlockId;
    type Value = BlockStatus;

    fn db_name() -> &'static str {
        "BlockStatus"
    }
}

impl Table for BlockHeaderTable {
    type Key = GlobalBlockId;
    type Value = v1alpha2::BlockHeader;

    fn db_name() -> &'static str {
        "BlockHeader"
    }
}

impl TableKey for ContractAtBlockId {
    type Encoded = [u8; 72];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 72];
        out[..40].copy_from_slice(&self.block_id.encode());
        out[40..].copy_from_slice(&self.contract_address.to_bytes());
        out
    }

    fn decode(b: &[u8]) -> Result<Self, apibara_node::db::KeyDecodeError> {
        let block_id = GlobalBlockId::decode(&b[..40])?;

        let contract_address = v1alpha2::FieldElement::from_slice(&b[40..]).map_err(|_| {
            KeyDecodeError::InvalidByteSize {
                expected: 72,
                actual: b.len(),
            }
        })?;

        Ok(ContractAtBlockId {
            block_id,
            contract_address,
        })
    }
}
