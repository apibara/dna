//! Core types used in this crate.

use core::fmt;

use apibara_node::db::{tables::BlockHash, KeyDecodeError};
use starknet::core::types::FieldElement;

mod pb {
    tonic::include_proto!("apibara.starknet.v1alpha1");
}

pub use self::pb::*;

#[derive(Clone, Copy, PartialEq)]
pub struct StarkNetBlockHash([u8; 32]);

impl BlockHash for StarkNetBlockHash {
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
        Ok(StarkNetBlockHash(element.to_bytes_be()))
    }
}

impl TryFrom<&StarkNetBlockHash> for FieldElement {
    type Error = KeyDecodeError;

    fn try_from(value: &StarkNetBlockHash) -> Result<Self, Self::Error> {
        let element = FieldElement::from_bytes_be(&value.0)
            .map_err(|err| KeyDecodeError::Other(Box::new(err)))?;
        Ok(element)
    }
}

impl AsRef<[u8]> for StarkNetBlockHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl fmt::Display for StarkNetBlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl fmt::Debug for StarkNetBlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StarkNetBlockHash(0x{})", hex::encode(&self.0))
    }
}
