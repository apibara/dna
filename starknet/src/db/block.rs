//! Block data.

use std::io::Cursor;

use apibara_node::db::{KeyDecodeError, Table, TableKey};
use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

use crate::core::{pb::v1alpha2, BlockHash, GlobalBlockId};

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
