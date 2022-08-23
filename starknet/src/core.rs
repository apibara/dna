//! Core types used in this crate.

use apibara_node::db::{tables::BlockHash, KeyDecodeError};
use starknet::core::types::FieldElement;

#[derive(Debug, Clone, Copy, PartialEq)]
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

impl AsRef<[u8]> for StarkNetBlockHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}
