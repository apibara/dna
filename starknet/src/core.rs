use std::fmt::{Debug, Display};

use starknet::core::types::{FieldElement, FromByteArrayError};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BlockHash([u8; 32]);

/// Global identifier for blocks.
#[derive(Copy, Clone, PartialEq)]
pub struct GlobalBlockId(u64, BlockHash);

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

pub mod pb {
    pub mod v1alpha2 {
        tonic::include_proto!("apibara.starknet.v1alpha2");

        pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("starknet_descriptor_v1alpha2");

        pub fn starknet_file_descriptor_set() -> &'static [u8] {
            FILE_DESCRIPTOR_SET
        }

        impl std::fmt::Display for BlockHash {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "0x{}", hex::encode(&self.hash))
            }
        }

        impl BlockStatus {
            pub fn is_finalized(&self) -> bool {
                *self == BlockStatus::AcceptedOnL1
            }
        }
    }
}

impl BlockHash {
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

    pub fn from_block(block: &pb::v1alpha2::Block) -> Result<Self, InvalidBlock> {
        let header = block.header.as_ref().ok_or(InvalidBlock::MissingHeader)?;
        let hash = header
            .block_hash
            .as_ref()
            .ok_or(InvalidBlock::MissingHash)?;
        let hash = hash.try_into()?;
        Ok(Self::new(header.block_number, hash))
    }

    pub fn number(&self) -> u64 {
        self.0
    }

    pub fn hash(&self) -> &BlockHash {
        &self.1
    }
}

impl TryFrom<&BlockHash> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: &BlockHash) -> Result<Self, Self::Error> {
        FieldElement::from_bytes_be(&value.0)
    }
}

impl TryFrom<&FieldElement> for BlockHash {
    type Error = InvalidBlockHashSize;

    fn try_from(value: &FieldElement) -> Result<Self, Self::Error> {
        let value = value.to_bytes_be();
        BlockHash::from_slice(&value)
    }
}

impl TryFrom<&self::pb::v1alpha2::BlockHash> for BlockHash {
    type Error = InvalidBlockHashSize;

    fn try_from(value: &self::pb::v1alpha2::BlockHash) -> Result<Self, Self::Error> {
        BlockHash::from_slice(&value.hash)
    }
}

impl From<&BlockHash> for self::pb::v1alpha2::BlockHash {
    fn from(h: &BlockHash) -> Self {
        use self::pb::v1alpha2;

        let hash = h.as_bytes().to_vec();

        v1alpha2::BlockHash { hash }
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
