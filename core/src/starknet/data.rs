use std::{
    fmt::Display,
    hash::{Hash, Hasher},
};

use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};
use starknet::core::types::{FieldElement as Felt, FromByteArrayError};

use super::proto::v1alpha2::*;

impl BlockStatus {
    pub fn is_finalized(&self) -> bool {
        *self == BlockStatus::AcceptedOnL1
    }

    pub fn is_accepted(&self) -> bool {
        *self == BlockStatus::AcceptedOnL2
    }

    pub fn is_rejected(&self) -> bool {
        *self == BlockStatus::Rejected
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FieldElementDecodeError {
    #[error("missing 0x prefix")]
    MissingPrefix,
    #[error("field element size is invalid")]
    InvalidSize,
    #[error("hex decode error: {0}")]
    DecodeError(#[from] hex::FromHexError),
}

impl FieldElement {
    /// Returns a new field element representing the given u64 value.
    pub fn from_u64(value: u64) -> FieldElement {
        FieldElement {
            lo_lo: 0,
            lo_hi: 0,
            hi_lo: 0,
            hi_hi: value,
        }
    }

    /// Returns a new field element from the raw byte representation.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        let lo_lo = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let lo_hi = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let hi_lo = u64::from_be_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);
        let hi_hi = u64::from_be_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
        ]);

        FieldElement {
            lo_lo,
            lo_hi,
            hi_lo,
            hi_hi,
        }
    }

    pub fn from_hex(s: &str) -> Result<Self, FieldElementDecodeError> {
        // must be at least 0x
        if !s.starts_with("0x") {
            return Err(FieldElementDecodeError::MissingPrefix);
        }

        // hex requires the string to be even-sized. If it's not, we copy it and add a leading 0.
        let bytes = if s.len() % 2 == 1 {
            let even_sized = format!("0{}", &s[2..]);
            hex::decode(even_sized)?
        } else {
            // skip 0x prefix
            hex::decode(&s[2..])?
        };

        // number is too big
        let size = bytes.len();
        if size > 32 {
            return Err(FieldElementDecodeError::InvalidSize);
        }
        let mut bytes_array = [0u8; 32];
        bytes_array[32 - size..].copy_from_slice(&bytes);
        Ok(FieldElement::from_bytes(&bytes_array))
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        let lo_lo = self.lo_lo.to_be_bytes();
        let lo_hi = self.lo_hi.to_be_bytes();
        let hi_lo = self.hi_lo.to_be_bytes();
        let hi_hi = self.hi_hi.to_be_bytes();
        [
            lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6], lo_lo[7],
            lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5], lo_hi[6], lo_hi[7],
            hi_lo[0], hi_lo[1], hi_lo[2], hi_lo[3], hi_lo[4], hi_lo[5], hi_lo[6], hi_lo[7],
            hi_hi[0], hi_hi[1], hi_hi[2], hi_hi[3], hi_hi[4], hi_hi[5], hi_hi[6], hi_hi[7],
        ]
    }

    /// Returns the field element as an hex string with 0x prefix.
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.to_bytes()))
    }
}

impl Display for FieldElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl Hash for FieldElement {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_bytes().hash(state);
    }
}

impl Serialize for FieldElement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for FieldElement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let fe = FieldElement::from_hex(&s).map_err(serde::de::Error::custom)?;
        Ok(fe)
    }
}

impl TryFrom<&FieldElement> for Felt {
    type Error = FromByteArrayError;

    fn try_from(value: &FieldElement) -> Result<Self, Self::Error> {
        Felt::from_bytes_be(&value.to_bytes())
    }
}

impl From<Felt> for FieldElement {
    fn from(felt: Felt) -> Self {
        (&felt).into()
    }
}

impl From<&Felt> for FieldElement {
    fn from(felt: &Felt) -> Self {
        let bytes = felt.to_bytes_be();
        FieldElement::from_bytes(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use starknet::core::types::FieldElement as Felt;

    use crate::starknet::v1alpha2::FieldElement;

    #[quickcheck]
    fn test_felt_from_u64(num: u64) {
        let felt = FieldElement::from_u64(num);
        let bytes = felt.to_bytes();
        // since it's a u64 it will never use bytes 0..24
        assert_eq!(bytes[0..24], [0; 24]);

        let back = FieldElement::from_bytes(&bytes);
        assert_eq!(back, felt);

        let as_hex = felt.to_hex();
        let back_hex = FieldElement::from_hex(&as_hex).unwrap();
        assert_eq!(felt, back_hex);
    }

    #[test]
    fn test_conversion_to_felt() {
        let two = Felt::MAX;
        let felt: FieldElement = two.into();
        assert_eq!(felt.lo_lo, 576460752303423505);
        assert_eq!(felt.lo_hi, 0);
        assert_eq!(felt.hi_lo, 0);
        assert_eq!(felt.hi_hi, 0);

        let as_hex = felt.to_hex();
        let back = FieldElement::from_hex(&as_hex).unwrap();
        assert_eq!(felt, back);
    }

    #[test]
    fn test_from_hex() {
        let felt = FieldElement::from_hex("0x1").unwrap();
        assert_eq!(felt.lo_lo, 0);
        assert_eq!(felt.lo_hi, 0);
        assert_eq!(felt.hi_lo, 0);
        assert_eq!(felt.hi_hi, 1);
    }
}
