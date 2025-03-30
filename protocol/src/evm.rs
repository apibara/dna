use error_stack::{report, Result, ResultExt};

use crate::helpers::{
    from_be_bytes_slice, impl_from_to_bytes, impl_scalar_helpers, impl_scalar_traits,
};

tonic::include_proto!("evm.v2");

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

impl U128 {
    pub fn from_u64(value: u64) -> Self {
        Self { x0: 0, x1: value }
    }

    pub fn from_u128(value: u128) -> Self {
        Self::from_bytes(&value.to_be_bytes())
    }
}

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

        let x = U128::from_u64(1234);
        let h = x.to_hex();
        assert_eq!("0x000000000000000000000000000004d2", &h);
    }
}
