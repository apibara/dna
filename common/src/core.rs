use apibara_dna_protocol::dna;
use rkyv::{Archive, Deserialize, Serialize};

/// Arbitrary length hash.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize, Default)]
pub struct Hash(pub Vec<u8>);

impl Hash {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Cursor uniquely identifies a block by its number and hash.
#[derive(Clone, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
pub struct Cursor {
    pub number: u64,
    pub hash: Hash,
}

pub trait GetCursor {
    /// Returns the current cursor.
    fn cursor(&self) -> Option<Cursor>;
}

impl Cursor {
    pub fn new_finalized(number: u64) -> Self {
        Self {
            number,
            hash: Default::default(),
        }
    }

    pub fn new_pending(number: u64) -> Self {
        Self {
            number,
            hash: Default::default(),
        }
    }

    pub fn strict_before(&self, other: &Self) -> bool {
        self.number < other.number
    }

    pub fn strict_after(&self, other: &Self) -> bool {
        self.number > other.number
    }

    pub fn new(number: u64, hash: Hash) -> Self {
        Self { number, hash }
    }

    pub fn hash_as_hex(&self) -> String {
        format!("{}", self.hash)
    }

    pub fn is_equivalent(&self, other: &Self) -> bool {
        if self.number != other.number {
            return false;
        }

        if self.hash.is_empty() || other.hash.is_empty() {
            return true;
        }

        if self.hash.len() == other.hash.len() {
            return self.hash == other.hash;
        }

        // Check that the two hashes end with the same bytes and that the longer hash extra bytes are all zero.
        let min_len = std::cmp::min(self.hash.len(), other.hash.len());

        let prefix = &self.hash.0[self.hash.len() - min_len..];
        let other_prefix = &other.hash.0[other.hash.len() - min_len..];

        if self.hash.len() > other.hash.len() {
            let len_diff = self.hash.len() - other.hash.len();
            return other_prefix == prefix && self.hash.0[0..len_diff] == vec![0; len_diff];
        }

        let len_diff = other.hash.len() - self.hash.len();
        prefix == other_prefix && other.hash.0[0..len_diff] == vec![0; len_diff]
    }
}

impl Hash {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|b| *b == 0)
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

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", self)
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return write!(f, "0x0");
        }
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl std::fmt::Display for ArchivedHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return write!(f, "0x0");
        }
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl From<Cursor> for dna::stream::Cursor {
    fn from(value: Cursor) -> Self {
        Self {
            order_key: value.number,
            unique_key: value.hash.0,
        }
    }
}

impl From<dna::stream::Cursor> for Cursor {
    fn from(value: dna::stream::Cursor) -> Self {
        Self {
            number: value.order_key,
            hash: Hash(value.unique_key),
        }
    }
}

pub mod testing {
    /// Returns a new test cursor where the hash depends on the cursor number and chain.
    pub fn new_test_cursor(number: u64, chain: u8) -> super::Cursor {
        let formatted = format!("{number:016x}{chain:02x}");
        let hash = hex::decode(formatted).expect("valid hash");
        super::Cursor {
            number,
            hash: super::Hash(hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Hash;

    use super::Cursor;

    #[test]
    fn test_cursor_is_equivalent() {
        {
            let cursor = Cursor::new(1, Hash::default());
            let other = Cursor::new(1, Hash([0, 0, 1].to_vec()));
            assert!(cursor.is_equivalent(&other));
            assert!(other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash::default());
            let other = Cursor::new(1, Hash::default());
            assert!(cursor.is_equivalent(&other));
            assert!(other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash::default());
            let other = Cursor::new(2, Hash::default());
            assert!(!cursor.is_equivalent(&other));
            assert!(!other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash([0, 0, 1].to_vec()));
            let other = Cursor::new(1, Hash([0, 1].to_vec()));
            assert!(cursor.is_equivalent(&other));
            assert!(other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash([0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4].to_vec()));
            let other = Cursor::new(1, Hash([1, 2, 3, 4].to_vec()));
            assert!(cursor.is_equivalent(&other));
            assert!(other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash([0, 0, 1].to_vec()));
            let other = Cursor::new(1, Hash([0, 0, 2].to_vec()));
            assert!(!cursor.is_equivalent(&other));
            assert!(!other.is_equivalent(&cursor));
        }

        {
            let cursor = Cursor::new(1, Hash([0, 0, 1].to_vec()));
            let other = Cursor::new(1, Hash([0, 0, 1, 1].to_vec()));
            assert!(!cursor.is_equivalent(&other));
            assert!(!other.is_equivalent(&cursor));
        }
    }
}
