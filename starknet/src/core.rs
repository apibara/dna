//! Core types used in this crate.

use core::fmt;

use apibara_node::db::{tables, KeyDecodeError};
use starknet::core::types::FieldElement;

mod pb {
    tonic::include_proto!("apibara.starknet.v1alpha1");
}

pub use self::pb::*;

impl tables::BlockHash for BlockHash {
    fn from_slice(b: &[u8]) -> Result<Self, KeyDecodeError> {
        if b.len() != 32 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 32,
                actual: b.len(),
            });
        }
        let mut bin = [0; 32];
        bin.copy_from_slice(&b[..32]);
        // Parse field element to check the hash is in the StarkNet field.
        let element = FieldElement::from_bytes_be(&bin)
            .map_err(|err| KeyDecodeError::Other(Box::new(err)))?;
        let block_hash = BlockHash {
            hash: element.to_bytes_be().to_vec(),
        };
        Ok(block_hash)
    }

    fn zero() -> Self {
        let hash = vec![0; 32];
        BlockHash { hash }
    }
}

impl tables::Block for Block {
    type Hash = BlockHash;

    fn number(&self) -> u64 {
        self.block_number
    }

    fn hash(&self) -> &Self::Hash {
        self.block_hash.as_ref().expect("missing field block_hash")
    }

    fn parent_hash(&self) -> &Self::Hash {
        self.parent_block_hash
            .as_ref()
            .expect("missing field parent_block_hash")
    }
}

impl TryFrom<&BlockHash> for FieldElement {
    type Error = KeyDecodeError;

    fn try_from(value: &BlockHash) -> Result<Self, Self::Error> {
        if value.hash.len() != 32 {
            return Err(KeyDecodeError::InvalidByteSize {
                expected: 32,
                actual: value.hash.len(),
            });
        }
        let mut bin = [0; 32];
        bin.copy_from_slice(&value.hash[..32]);
        let element = FieldElement::from_bytes_be(&bin)
            .map_err(|err| KeyDecodeError::Other(Box::new(err)))?;
        Ok(element)
    }
}

impl AsRef<[u8]> for BlockHash {
    fn as_ref(&self) -> &[u8] {
        self.hash.as_ref()
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.hash))
    }
}
