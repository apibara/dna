use std::fmt::{Debug, Display};

use apibara_core::{node::v1alpha2::Cursor as ProtoCursor, starknet::v1alpha2};
use apibara_node::{core::Cursor, stream::IngestionMessage as GenericIngestionMessage};
use starknet::core::types::{FieldElement, FromByteArrayError};

#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub struct BlockHash([u8; 32]);

/// Global identifier for blocks.
#[derive(Copy, Clone, PartialEq)]
pub struct GlobalBlockId(u64, BlockHash);

pub type IngestionMessage = GenericIngestionMessage<GlobalBlockId>;

#[derive(Debug, thiserror::Error)]
#[error("invalid block hash size")]
pub struct InvalidBlockHashSize {
    pub expected: usize,
    pub actual: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidBlock {
    #[error("missing block header")]
    MissingHeader,
    #[error("missing block hash")]
    MissingHash,
    #[error(transparent)]
    InvalidHash(#[from] InvalidBlockHashSize),
}

impl BlockHash {
    pub fn zero() -> Self {
        BlockHash([0; 32])
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::zero()
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, InvalidBlockHashSize> {
        if b.len() != 32 {
            return Err(InvalidBlockHashSize {
                expected: 32,
                actual: b.len(),
            });
        }
        let mut out = [0; 32];
        out.copy_from_slice(b);
        Ok(BlockHash(out))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl GlobalBlockId {
    pub fn new(number: u64, hash: BlockHash) -> Self {
        GlobalBlockId(number, hash)
    }

    pub fn from_cursor(cursor: &ProtoCursor) -> Result<Self, InvalidBlockHashSize> {
        let hash = if cursor.unique_key.is_empty() {
            BlockHash::zero()
        } else {
            BlockHash::from_slice(&cursor.unique_key)?
        };
        Ok(Self::new(cursor.order_key, hash))
    }

    pub fn from_block(block: &v1alpha2::Block) -> Result<Self, InvalidBlock> {
        let header = block.header.as_ref().ok_or(InvalidBlock::MissingHeader)?;
        Self::from_block_header(header)
    }

    pub fn from_block_header(header: &v1alpha2::BlockHeader) -> Result<Self, InvalidBlock> {
        let hash = header
            .block_hash
            .as_ref()
            .ok_or(InvalidBlock::MissingHash)?;
        let hash = hash.into();
        Ok(Self::new(header.block_number, hash))
    }

    pub fn from_block_header_parent(header: &v1alpha2::BlockHeader) -> Result<Self, InvalidBlock> {
        let hash = header
            .parent_block_hash
            .as_ref()
            .ok_or(InvalidBlock::MissingHash)?;
        let hash = hash.into();
        Ok(Self::new(header.block_number - 1, hash))
    }

    pub fn number(&self) -> u64 {
        self.0
    }

    pub fn hash(&self) -> &BlockHash {
        &self.1
    }

    /// Returns a cursor corresponding to the block id.
    pub fn to_cursor(&self) -> ProtoCursor {
        ProtoCursor {
            order_key: self.number(),
            unique_key: self.hash().as_bytes().to_vec(),
        }
    }
}

impl From<v1alpha2::FieldElement> for BlockHash {
    fn from(felt: v1alpha2::FieldElement) -> Self {
        (&felt).into()
    }
}

impl From<&v1alpha2::FieldElement> for BlockHash {
    fn from(felt: &v1alpha2::FieldElement) -> Self {
        BlockHash(felt.to_bytes())
    }
}

impl TryFrom<&BlockHash> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: &BlockHash) -> Result<Self, Self::Error> {
        FieldElement::from_bytes_be(&value.0)
    }
}

impl From<&BlockHash> for v1alpha2::FieldElement {
    fn from(hash: &BlockHash) -> Self {
        Self::from_bytes(&hash.0)
    }
}

impl Display for GlobalBlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hash = hex::encode(self.hash().as_bytes());
        write!(f, "{}/0x{}", self.number(), hash)
    }
}

impl Debug for GlobalBlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GBI({})", self)
    }
}

impl Default for GlobalBlockId {
    fn default() -> Self {
        Self::new(0, BlockHash::zero())
    }
}

impl apibara_node::core::Cursor for GlobalBlockId {
    fn from_proto(cursor: &ProtoCursor) -> Option<Self> {
        GlobalBlockId::from_cursor(cursor).ok()
    }

    fn to_proto(&self) -> ProtoCursor {
        self.to_cursor()
    }
}

