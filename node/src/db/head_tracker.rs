//! Head tracker related tables

use std::{io::Cursor, marker::PhantomData};

use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

use super::{table::KeyDecodeError, Table, TableKey};

pub trait BlockHash:
    Send + Sync + AsRef<[u8]> + Sized + PartialEq + Clone + std::fmt::Debug
{
    fn from_slice(b: &[u8]) -> Result<Self, KeyDecodeError>;
}

/// Table with the block headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockHeaderTable<H> {
    phantom: PhantomData<H>,
}

/// Table with the block hashes of the canonical chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct CanonicalBlockHeaderTable<H> {
    phantom: PhantomData<H>,
}

/// Basic block header information. This information is used only to track the
/// canonical chain and as such it doesn't need the complete information about a
/// block.
#[derive(Clone, PartialEq, Message)]
pub struct BlockHeader {
    #[prost(uint64, tag = "1")]
    pub number: u64,
    #[prost(bytes, tag = "2")]
    pub hash: Vec<u8>,
    #[prost(bytes, tag = "3")]
    pub parent_hash: Vec<u8>,
}

/// Hash of a block belonging to the canonical chain.
#[derive(Clone, PartialEq, Message)]
pub struct CanonicalBlockHeader {
    #[prost(bytes, tag = "1")]
    pub hash: Vec<u8>,
}

impl<H> Table for CanonicalBlockHeaderTable<H>
where
    H: Send + Sync,
{
    type Key = u64;
    type Value = CanonicalBlockHeader;

    fn db_name() -> &'static str {
        "CanonicalBlockHeader"
    }
}

impl<H> Table for BlockHeaderTable<H>
where
    H: Send + Sync + BlockHash,
{
    type Key = (u64, H);
    type Value = BlockHeader;

    fn db_name() -> &'static str {
        "BlockHeader"
    }
}

impl<H> TableKey for (u64, H)
where
    H: BlockHash,
{
    type Encoded = Vec<u8>;

    fn encode(&self) -> Self::Encoded {
        let mut out = Vec::new();
        out.extend_from_slice(&self.0.to_be_bytes());
        out.extend_from_slice(self.1.as_ref());
        out
    }

    fn decode(b: &[u8]) -> Result<Self, KeyDecodeError> {
        let mut cursor = Cursor::new(b);
        let block_number = cursor
            .read_u64::<BigEndian>()
            .map_err(KeyDecodeError::ReadError)?;
        let block_hash = H::from_slice(&b[8..])?;
        Ok((block_number, block_hash))
    }
}
