//! Head tracker related tables

use std::{io::Cursor, marker::PhantomData};

use byteorder::{BigEndian, ReadBytesExt};
use prost::Message;

use super::{table::KeyDecodeError, Table, TableKey};

/// A block's hash.
pub trait BlockHash:
    Send + Sync + AsRef<[u8]> + Sized + PartialEq + Clone + std::fmt::Debug
{
    fn from_slice(b: &[u8]) -> Result<Self, KeyDecodeError>;
}

/// A blockchain block with the associated hash type.
pub trait Block: Send + Sync + Default + prost::Message + std::fmt::Debug {
    type Hash: BlockHash;

    /// Returns the block number or height.
    fn number(&self) -> u64;

    /// Returns the block hash.
    fn hash(&self) -> &Self::Hash;

    /// Returns the block's parent hash.
    fn parent_hash(&self) -> &Self::Hash;
}

/// Table with the block headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockTable<B: Block> {
    phantom: PhantomData<B>,
}

/// Table with the block hashes of the canonical chain.
#[derive(Debug, Clone, Copy, Default)]
pub struct CanonicalBlockTable<H: BlockHash> {
    phantom: PhantomData<H>,
}

/// Hash of a block belonging to the canonical chain.
#[derive(Clone, PartialEq, Message)]
pub struct CanonicalBlock {
    #[prost(bytes, tag = "1")]
    pub hash: Vec<u8>,
}

impl<H> Table for CanonicalBlockTable<H>
where
    H: Send + Sync + BlockHash,
{
    type Key = u64;
    type Value = CanonicalBlock;

    fn db_name() -> &'static str {
        "CanonicalBlock"
    }
}

impl<B> Table for BlockTable<B>
where
    B: Send + Sync + Block,
{
    type Key = (u64, B::Hash);
    type Value = B;

    fn db_name() -> &'static str {
        "Block"
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
