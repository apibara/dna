use crate::{ingestion::models, segment::store};

impl From<models::H256> for store::B256 {
    fn from(value: models::H256) -> Self {
        store::B256(value.0)
    }
}

impl From<&models::H256> for store::B256 {
    fn from(value: &models::H256) -> Self {
        store::B256(value.0)
    }
}

impl From<models::U256> for store::U256 {
    fn from(value: models::U256) -> Self {
        let mut out = [0; 32];
        value.to_big_endian(&mut out);
        store::U256(out)
    }
}

impl From<u128> for store::U128 {
    fn from(value: u128) -> Self {
        store::U128(value.to_be_bytes())
    }
}

impl From<models::H160> for store::Address {
    fn from(value: models::H160) -> Self {
        store::Address(value.0)
    }
}

impl From<&models::H160> for store::Address {
    fn from(value: &models::H160) -> Self {
        store::Address(value.0)
    }
}

impl From<models::Bloom> for store::Bloom {
    fn from(value: models::Bloom) -> Self {
        store::Bloom(value.0)
    }
}

pub trait H64Ext {
    fn into_u64(self) -> u64;
}

pub trait U256Ext {
    fn into_u128(self) -> store::U128;
}

impl H64Ext for models::H64 {
    fn into_u64(self) -> u64 {
        u64::from_be_bytes(self.0)
    }
}

impl U256Ext for models::U256 {
    fn into_u128(self) -> store::U128 {
        let bytes = self.as_u128().to_be_bytes();
        store::U128(bytes)
    }
}
