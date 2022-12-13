use std::fmt::{Debug, Display};

use starknet::core::types::{FieldElement, FromByteArrayError};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BlockHash([u8; 32]);

/// Global identifier for blocks.
#[derive(Copy, Clone, PartialEq)]
pub struct GlobalBlockId(u64, BlockHash);

#[derive(Debug, Clone)]
pub enum IngestionMessage {
    Finalized(GlobalBlockId),
    Accepted(GlobalBlockId),
}

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
    pub mod starknet {
        pub mod v1alpha2 {
            tonic::include_proto!("apibara.starknet.v1alpha2");

            pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
                tonic::include_file_descriptor_set!("starknet_descriptor_v1alpha2");

            pub fn starknet_file_descriptor_set() -> &'static [u8] {
                FILE_DESCRIPTOR_SET
            }

            impl BlockStatus {
                pub fn is_finalized(&self) -> bool {
                    *self == BlockStatus::AcceptedOnL1
                }
            }

            impl FieldElement {
                pub fn from_u64(value: u64) -> FieldElement {
                    FieldElement {
                        lo_lo: 0,
                        lo_hi: 0,
                        hi_lo: 0,
                        hi_hi: value,
                    }
                }

                pub fn from_bytes(bytes: &[u8; 32]) -> Self {
                    let lo_lo = u64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]);
                    let lo_hi = u64::from_be_bytes([
                        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14],
                        bytes[15],
                    ]);
                    let hi_lo = u64::from_be_bytes([
                        bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21],
                        bytes[22], bytes[23],
                    ]);
                    let hi_hi = u64::from_be_bytes([
                        bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29],
                        bytes[30], bytes[31],
                    ]);

                    FieldElement {
                        lo_lo,
                        lo_hi,
                        hi_lo,
                        hi_hi,
                    }
                }

                pub fn to_bytes(&self) -> [u8; 32] {
                    let lo_lo = self.lo_lo.to_be_bytes();
                    let lo_hi = self.lo_hi.to_be_bytes();
                    let hi_lo = self.hi_lo.to_be_bytes();
                    let hi_hi = self.hi_hi.to_be_bytes();
                    [
                        lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6],
                        lo_lo[7], lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5],
                        lo_hi[6], lo_hi[7], hi_lo[0], hi_lo[1], hi_lo[2], hi_lo[3], hi_lo[4],
                        hi_lo[5], hi_lo[6], hi_lo[7], hi_hi[0], hi_hi[1], hi_hi[2], hi_hi[3],
                        hi_hi[4], hi_hi[5], hi_hi[6], hi_hi[7],
                    ]
                }
            }
        }
    }

    pub mod stream {
        pub mod v1alpha2 {
            tonic::include_proto!("apibara.node.v1alpha2");

            pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
                tonic::include_file_descriptor_set!("starknet_descriptor_v1alpha2");

            pub fn stream_file_descriptor_set() -> &'static [u8] {
                FILE_DESCRIPTOR_SET
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

    pub fn from_block(block: &pb::starknet::v1alpha2::Block) -> Result<Self, InvalidBlock> {
        let header = block.header.as_ref().ok_or(InvalidBlock::MissingHeader)?;
        let hash = header
            .block_hash
            .as_ref()
            .ok_or(InvalidBlock::MissingHash)?;
        let hash = hash.into();
        Ok(Self::new(header.block_number, hash))
    }

    pub fn number(&self) -> u64 {
        self.0
    }

    pub fn hash(&self) -> &BlockHash {
        &self.1
    }
}

impl From<pb::starknet::v1alpha2::FieldElement> for BlockHash {
    fn from(felt: pb::starknet::v1alpha2::FieldElement) -> Self {
        (&felt).into()
    }
}

impl From<&pb::starknet::v1alpha2::FieldElement> for BlockHash {
    fn from(felt: &pb::starknet::v1alpha2::FieldElement) -> Self {
        BlockHash(felt.to_bytes())
    }
}

impl TryFrom<&pb::starknet::v1alpha2::FieldElement> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: &pb::starknet::v1alpha2::FieldElement) -> Result<Self, Self::Error> {
        FieldElement::from_bytes_be(&value.to_bytes())
    }
}

impl TryFrom<&BlockHash> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: &BlockHash) -> Result<Self, Self::Error> {
        FieldElement::from_bytes_be(&value.0)
    }
}

impl From<&BlockHash> for pb::starknet::v1alpha2::FieldElement {
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

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use starknet::core::types::FieldElement as Felt;

    use super::pb::starknet::v1alpha2::FieldElement;

    #[quickcheck]
    fn test_felt_from_u64(num: u64) {
        let felt = FieldElement::from_u64(num);
        let bytes = felt.to_bytes();
        // since it's a u64 it will never use bytes 0..24
        assert_eq!(bytes[0..24], [0; 24]);

        let back = FieldElement::from_bytes(&bytes);
        assert_eq!(back, felt);
    }

    #[test]
    fn test_conversion_to_felt() {
        let two = Felt::MAX;
        let felt: FieldElement = two.into();
        assert_eq!(felt.lo_lo, 576460752303423505);
        assert_eq!(felt.lo_hi, 0);
        assert_eq!(felt.hi_lo, 0);
        assert_eq!(felt.hi_hi, 0);
    }
}
