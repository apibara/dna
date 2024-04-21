//! Conversion between store and model.
use apibara_dna_common::core::Cursor;

use crate::{ingestion::models, segment::store};

impl From<models::B256> for store::B256 {
    fn from(value: models::B256) -> Self {
        store::B256(value.0)
    }
}

impl From<&models::B256> for store::B256 {
    fn from(value: &models::B256) -> Self {
        store::B256(value.0)
    }
}

impl From<models::U256> for store::U256 {
    fn from(value: models::U256) -> Self {
        store::U256(value.to_be_bytes())
    }
}

impl From<models::U128> for store::U128 {
    fn from(value: models::U128) -> Self {
        store::U128(value.to_be_bytes())
    }
}

impl From<u128> for store::U128 {
    fn from(value: u128) -> Self {
        store::U128(value.to_be_bytes())
    }
}

impl From<models::Address> for store::Address {
    fn from(value: models::Address) -> Self {
        store::Address(value.0.into())
    }
}

impl From<&models::Address> for store::Address {
    fn from(value: &models::Address) -> Self {
        store::Address(value.0.into())
    }
}

impl From<models::Bloom> for store::Bloom {
    fn from(value: models::Bloom) -> Self {
        store::Bloom(value.0.into())
    }
}

pub trait U64Ext {
    fn as_u64(&self) -> u64;
}

pub trait U128Ext {
    fn into_u128(self) -> store::U128;
}

impl U64Ext for models::B64 {
    fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

impl U64Ext for models::U8 {
    fn as_u64(&self) -> u64 {
        let bytes: [u8; 1] = self.to_be_bytes();
        bytes[0] as u64
    }
}

impl U64Ext for models::U64 {
    fn as_u64(&self) -> u64 {
        u64::from_be_bytes(self.to_be_bytes())
    }
}

impl U64Ext for models::U256 {
    fn as_u64(&self) -> u64 {
        if self.leading_zeros() < (32 - 8) * 8 {
            panic!("u256 too large to fit in u64")
        }
        let bytes: [u8; 32] = self.to_be_bytes();
        let mut out = [0; 8];
        out.copy_from_slice(&bytes[24..]);
        u64::from_be_bytes(out)
    }
}

impl U128Ext for models::U128 {
    fn into_u128(self) -> store::U128 {
        let bytes = self.to_be_bytes();
        store::U128(bytes)
    }
}

pub trait GetCursor {
    fn cursor(&self) -> Option<Cursor>;
}

impl GetCursor for models::Header {
    fn cursor(&self) -> Option<Cursor> {
        let Some(hash) = self.hash else {
            return None;
        };

        let Some(number) = self.number else {
            return None;
        };

        Some(Cursor::new(number.as_u64(), hash.to_vec()))
    }
}

impl GetCursor for models::Block {
    fn cursor(&self) -> Option<Cursor> {
        self.header.cursor()
    }
}

#[cfg(test)]
mod tests {
    use crate::ingestion::models;

    use super::U64Ext;

    #[test]
    fn test_u256_to_u64_conversion() {
        let n = 0u64;
        let nm = models::U256::from(n);
        assert_eq!(nm.as_u64(), n);

        let n = u64::MAX;
        let nm = models::U256::from(n);
        assert_eq!(nm.as_u64(), n);
    }

    #[test]
    #[should_panic]
    fn test_u256_to_u64_conversion_panics_if_too_big() {
        let n = u64::MAX as u128;
        let nm = models::U256::from(n + 1);
        let _ = nm.as_u64();
    }

    #[test]
    fn test_u64_to_u64_conversion() {
        let n = 0u64;
        let nm = models::U64::from(n);
        assert_eq!(nm.as_u64(), n);

        let n = u64::MAX;
        let nm = models::U64::from(n);
        assert_eq!(nm.as_u64(), n);
    }

    #[test]
    fn test_u8_to_u64_conversion() {
        for i in 0..u8::MAX {
            let nm = models::U8::from(i);
            assert_eq!(nm.as_u64() as u8, i);
        }
    }
}
