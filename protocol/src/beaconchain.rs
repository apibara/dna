use error_stack::{report, Result, ResultExt};

use crate::helpers::{
    from_be_bytes_slice, impl_from_to_bytes, impl_scalar_helpers, impl_scalar_traits,
};

tonic::include_proto!("beaconchain.v2");

impl_scalar_traits!(Address);
impl_from_to_bytes!(Address, 20);
impl_scalar_helpers!(Address, 20);

impl_scalar_traits!(U256);
impl_from_to_bytes!(U256, 32);
impl_scalar_helpers!(U256, 32);

impl_scalar_traits!(B256);
impl_from_to_bytes!(B256, 32);
impl_scalar_helpers!(B256, 32);

impl_scalar_traits!(U128);
impl_from_to_bytes!(U128, 16);
impl_scalar_helpers!(U128, 16);

impl_scalar_traits!(B384);
impl_from_to_bytes!(B384, 48);
impl_scalar_helpers!(B384, 48);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_address() {
        let hex = "0x27504265a9bc4330e3fe82061a60cd8b6369b4dc";
        let address = Address::from_hex(hex).unwrap();
        let back = address.to_hex();
        assert_eq!(hex, &back);
    }

    #[test]
    pub fn test_u256() {
        let hex = "0x9df92d765b5aa041fd4bbe8d5878eb89290efa78e444c1a603eecfae2ea05fa4";
        let u256 = U256::from_hex(hex).unwrap();
        let back = u256.to_hex();
        assert_eq!(hex, &back);
    }

    #[test]
    pub fn test_b256() {
        let hex = "0x9df92d765b5aa041fd4bbe8d5878eb89290efa78e444c1a603eecfae2ea05fa4";
        let b256 = B256::from_hex(hex).unwrap();
        let back = b256.to_hex();
        assert_eq!(hex, &back);
    }

    #[test]
    pub fn test_u128() {
        let hex = "0x090efa78e444c1a603eecfae2ea05fa4";
        let u256 = U128::from_hex(hex).unwrap();
        let back = u256.to_hex();
        assert_eq!(hex, &back);
    }

    #[test]
    pub fn test_b384() {
        let hex = "0xa5ea8a2ab0dd059fe4768323f64bf271ded6ac61df171735c72022f8e9ecfea54bb5da2a46d3fd1e57146eecbe2e38bd";
        let u256 = B384::from_hex(hex).unwrap();
        let back = u256.to_hex();
        assert_eq!(hex, &back);
    }
}
