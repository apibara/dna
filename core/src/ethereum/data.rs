use ethers_core::types::H256 as EthersH256;

use crate::bytes::bytes_to_4xu64;

use super::proto::v1alpha2::*;

impl H256 {
    /// Returns a new H256 representing the given u64 value.
    pub fn from_u64(value: u64) -> H256 {
        H256 {
            lo_lo: 0,
            lo_hi: 0,
            hi_lo: 0,
            hi_hi: value,
        }
    }

    /// Returns a new H256 from the raw byte representation.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        let (lo_lo, lo_hi, hi_lo, hi_hi) = bytes_to_4xu64(bytes);

        H256 {
            lo_lo,
            lo_hi,
            hi_lo,
            hi_hi,
        }
    }

    /// Returns the raw byte representation of the H256.
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

impl From<&EthersH256> for H256 {
    fn from(hash: &EthersH256) -> Self {
        let bytes = hash.as_fixed_bytes();
        H256::from_bytes(bytes)
    }
}

impl From<EthersH256> for H256 {
    fn from(hash: EthersH256) -> Self {
        let bytes = hash.as_fixed_bytes();
        H256::from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ethers_core::types::H256 as EthersH256;
    use quickcheck_macros::quickcheck;

    use crate::ethereum::v1alpha2::H256;

    #[quickcheck]
    fn test_h256_from_u64(num: u64) {
        let felt = H256::from_u64(num);
        let bytes = felt.to_bytes();
        // since it's a u64 it will never use bytes 0..24
        assert_eq!(bytes[0..24], [0; 24]);

        let back = H256::from_bytes(&bytes);
        assert_eq!(back, felt);
    }

    #[test]
    fn test_conversion_with_ethers_core() {
        let hash = EthersH256::from_str(
            "6e57d4533ee7c47010be7afec060a4e152f4cb26f0a3fb2fd3200f04161e8e1f",
        )
        .unwrap();
        let conv = H256::from(&hash);
        let back = EthersH256::from_str(&conv.to_hex()).unwrap();
        assert_eq!(hash, back);
    }
}
