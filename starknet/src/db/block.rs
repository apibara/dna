//! Block data.

use std::io::Cursor;

use apibara_core::starknet::v1alpha2;
use apibara_node::db::{Decodable, DecodeError, Encodable, Table};
use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

use crate::{
    core::{BlockHash, GlobalBlockId},
    db::serde::EncodedMessage,
};

#[derive(Clone, PartialEq, Message)]
pub struct BlockBody {
    #[prost(message, repeated, tag = "1")]
    pub transactions: prost::alloc::vec::Vec<v1alpha2::Transaction>,
}

#[derive(Clone, PartialEq, Message)]
pub struct HasherKeys {
    #[prost(fixed64, tag = "1")]
    pub hash0_0: u64,
    #[prost(fixed64, tag = "2")]
    pub hash0_1: u64,
    #[prost(fixed64, tag = "3")]
    pub hash1_0: u64,
    #[prost(fixed64, tag = "4")]
    pub hash1_1: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct RawBloom {
    #[prost(bytes, tag = "1")]
    pub bytes: prost::alloc::vec::Vec<u8>,
    #[prost(fixed64, tag = "2")]
    pub bitmap_bits: u64,
    #[prost(fixed32, tag = "3")]
    pub number_of_hash_functions: u32,
    #[prost(message, tag = "4")]
    pub hasher_keys: Option<HasherKeys>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlockReceipts {
    #[prost(message, repeated, tag = "1")]
    pub receipts: prost::alloc::vec::Vec<v1alpha2::TransactionReceipt>,
    #[prost(message, tag = "2")]
    pub bloom: Option<RawBloom>,
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

impl Encodable for BlockHash {
    type Encoded = [u8; 32];

    fn encode(&self) -> Self::Encoded {
        (*self).into_bytes()
    }
}

impl Decodable for BlockHash {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        BlockHash::from_slice(b).map_err(|err| DecodeError::InvalidByteSize {
            expected: err.expected,
            actual: err.actual,
        })
    }
}

// A pair (block number, block hash) is encoded as:
// - 8 bytes big endian representation of the block number
// - 32 bytes block hash
impl Encodable for GlobalBlockId {
    type Encoded = [u8; 40];

    fn encode(&self) -> Self::Encoded {
        let mut out = [0; 40];
        out[..8].copy_from_slice(&self.number().to_be_bytes());
        out[8..].copy_from_slice(self.hash().as_bytes());
        out
    }
}

impl Decodable for GlobalBlockId {
    fn decode(b: &[u8]) -> Result<Self, DecodeError> {
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(DecodeError::ReadError)?;
        let block_hash =
            BlockHash::from_slice(&b[8..]).map_err(|err| DecodeError::InvalidByteSize {
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
    type Value = EncodedMessage<v1alpha2::BlockHeader>;

    fn db_name() -> &'static str {
        "BlockHeader"
    }
}
