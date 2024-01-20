use std::fmt::Display;

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;

use super::store;

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
