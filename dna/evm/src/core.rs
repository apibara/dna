use crate::ingestion::models;

#[derive(Clone, PartialEq, Eq)]
pub struct Cursor {
    pub number: u64,
    pub hash: models::B256,
}

impl Cursor {
    pub fn new(number: u64, hash: models::B256) -> Self {
        Self { number, hash }
    }
}

impl std::fmt::Debug for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cursor(n={} h={})", self.number, self.hash)
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.number, self.hash)
    }
}
