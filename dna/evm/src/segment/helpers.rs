use std::str::FromStr;

use apibara_dna_common::{
    error::{DnaError, Result},
    flatbuffers::VectorExt,
};
use error_stack::ResultExt;
use ethers::utils::{ConversionError, Units};
use hex::ToHex;
use roaring::RoaringBitmap;

use crate::ingestion::models;

use super::store;

impl store::Address {
    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        store::Address(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let size = bytes.len();
        if size > 20 {
            return Err(DnaError::Fatal).attach_printable("expected 20 bytes for address");
        }
        let mut out = [0u8; 20];
        out[20 - size..].copy_from_slice(bytes);
        Ok(Self::from_bytes(out))
    }

    pub fn from_hex(hex: &str) -> Result<Self> {
        let address = models::H160::from_str(hex)
            .change_context(DnaError::Fatal)
            .attach_printable("failed to parse hex to address")?;
        Ok(address.into())
    }

    pub fn as_hex(&self) -> String {
        let address = models::H160::from(self.0);
        ethers::core::utils::to_checksum(&address, None)
    }
}

impl store::U256 {
    pub fn format_units<U>(&self, units: U) -> Result<String>
    where
        U: TryInto<Units, Error = ConversionError>,
    {
        let amount = models::U256::from(self.0);
        let formatted =
            ethers::core::utils::format_units(amount, units).change_context(DnaError::Fatal)?;
        Ok(formatted)
    }
}

impl store::B256 {
    pub fn as_hex(&self) -> String {
        let bytes = self.0;
        let hex = bytes.encode_hex::<String>();
        format!("0x{hex}")
    }
}

impl PartialOrd for store::Address {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for store::Address {}

impl Ord for store::Address {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl std::fmt::Display for store::Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_hex())
    }
}

pub trait SegmentGroupExt {
    fn get_log_by_address(&self, address: &store::Address) -> Option<RoaringBitmap>;
}

impl<'a> SegmentGroupExt for store::SegmentGroup<'a> {
    fn get_log_by_address(&self, address: &store::Address) -> Option<RoaringBitmap> {
        let logs = self.log_by_address().unwrap_or_default();
        let Some(bitmap_data) = logs
            .binary_search_by_key(address, |kv| kv.key())
            .and_then(|kv| kv.bitmap())
        else {
            return None;
        };
        let bitmap = RoaringBitmap::deserialize_from(bitmap_data.bytes())
            .expect("failed to deserialize bitmap");
        Some(bitmap)
    }
}
