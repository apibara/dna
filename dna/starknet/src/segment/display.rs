use std::fmt::Display;

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use flatbuffers::Follow;

use super::store;

pub trait VectorExt<'a, T>
where
    T: Follow<'a> + 'a,
{
    fn binary_search_by_key<K, F>(&self, key: &K, f: F) -> Option<<T as Follow<'a>>::Inner>
    where
        F: Fn(&<T as Follow<'a>>::Inner) -> Option<&'a K>,
        K: Ord + 'a;
}

impl<'a, T> VectorExt<'a, T> for flatbuffers::Vector<'a, T>
where
    T: Follow<'a> + 'a,
{
    fn binary_search_by_key<K, F>(&self, key: &K, f: F) -> Option<<T as Follow<'a>>::Inner>
    where
        F: Fn(&<T as Follow<'a>>::Inner) -> Option<&'a K>,
        K: Ord + 'a,
    {
        use std::cmp::Ordering::*;

        let mut size = self.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;
            let el = self.get(mid);
            let mid_key = f(&el).expect("key must be present");

            let cmp = mid_key.cmp(key);
            if cmp == Less {
                left = mid + 1;
            } else if cmp == Greater {
                right = mid;
            } else {
                return Some(el);
            }

            size = right - left;
        }

        None
    }
}

impl Display for store::FieldElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl store::FieldElement {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let size = bytes.len();
        if size > 32 {
            return Err(DnaError::Fatal).attach_printable("expected 32 bytes for felt");
        }
        let mut out = [0u8; 32];
        out[32 - size..].copy_from_slice(bytes);
        Ok(Self::from_bytes(out))
    }

    pub fn from_hex(hex: &str) -> Result<Self> {
        if !hex.starts_with("0x") {
            return Err(DnaError::Fatal).attach_printable("expected hex to start with 0x");
        }

        let bytes = if hex.len() % 2 == 1 {
            let even_sized = format!("0{}", &hex[2..]);
            hex::decode(even_sized)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to decode hex to felt")?
        } else {
            // skip 0x prefix
            hex::decode(&hex[2..])
                .change_context(DnaError::Fatal)
                .attach_printable("failed to decode hex to felt")?
        };

        Self::from_slice(&bytes)
    }
}

impl PartialOrd for store::FieldElement {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for store::FieldElement {}

impl Ord for store::FieldElement {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
