use apibara_dna_protocol::dna;

/// Cursor uniquely identifies a block by its number and hash.
#[derive(Clone, PartialEq, Eq)]
pub struct Cursor {
    pub number: u64,
    pub hash: Vec<u8>,
}

impl Cursor {
    pub fn new_finalized(number: u64) -> Self {
        Self {
            number,
            hash: Vec::new(),
        }
    }

    pub fn new(number: u64, hash: Vec<u8>) -> Self {
        Self { number, hash }
    }

    pub fn hash_as_hex(&self) -> String {
        if self.hash.is_empty() {
            return "0x0".to_string();
        }
        format!("0x{}", hex::encode(&self.hash))
    }
}

impl std::fmt::Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cursor(n={} h={})", self.number, self.hash_as_hex())
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.number, self.hash_as_hex())
    }
}

impl From<Cursor> for dna::common::Cursor {
    fn from(value: Cursor) -> Self {
        Self {
            order_key: value.number,
            unique_key: value.hash,
        }
    }
}

impl From<dna::common::Cursor> for Cursor {
    fn from(value: dna::common::Cursor) -> Self {
        Self {
            number: value.order_key,
            hash: value.unique_key,
        }
    }
}
