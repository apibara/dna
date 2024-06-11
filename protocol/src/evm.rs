use error_stack::{report, Result, ResultExt};

use crate::{
    error::DecodeError,
    helpers::{impl_scalar_helpers, impl_scalar_traits, impl_u256_scalar},
};

tonic::include_proto!("evm.v2");

impl_scalar_traits!(Address);
impl_scalar_helpers!(Address, 20);

impl Address {
    pub fn from_bytes(bytes: &[u8; 20]) -> Self {
        let lo_lo = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let lo_hi = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let hi = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

        Address { lo_lo, lo_hi, hi }
    }

    pub fn to_bytes(&self) -> [u8; 20] {
        let lo_lo = self.lo_lo.to_be_bytes();
        let lo_hi = self.lo_hi.to_be_bytes();
        let hi = self.hi.to_be_bytes();
        [
            lo_lo[0], lo_lo[1], lo_lo[2], lo_lo[3], lo_lo[4], lo_lo[5], lo_lo[6], lo_lo[7], //
            lo_hi[0], lo_hi[1], lo_hi[2], lo_hi[3], lo_hi[4], lo_hi[5], lo_hi[6], lo_hi[7], //
            hi[0], hi[1], hi[2], hi[3],
        ]
    }
}

impl_scalar_traits!(U128);
impl_scalar_helpers!(U128, 16);

impl U128 {
    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        let lo = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let hi = u64::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        U128 { lo, hi }
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        let lo = self.lo.to_be_bytes();
        let hi = self.hi.to_be_bytes();
        [
            lo[0], lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7], //
            hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], //
        ]
    }
}

impl_u256_scalar!(U256);
impl_u256_scalar!(B256);

impl_scalar_traits!(HexData);
impl_scalar_helpers!(HexData);

impl HexData {
    pub fn from_slice(bytes: &[u8]) -> Result<HexData, DecodeError> {
        Ok(HexData {
            value: bytes.to_vec(),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.value.clone()
    }
}
