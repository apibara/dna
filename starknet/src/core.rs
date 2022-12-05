use starknet::core::types::{FieldElement, FromByteArrayError};

#[derive(Debug, Clone)]
pub struct BlockHash([u8; 32]);

impl TryFrom<&BlockHash> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: &BlockHash) -> Result<Self, Self::Error> {
        FieldElement::from_bytes_be(&value.0)
    }
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
    }
}
