use crate::ingestion::models;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub number: u64,
    pub hash: models::B256,
}

impl Cursor {
    pub fn new(number: u64, hash: models::B256) -> Self {
        Self { number, hash }
    }
}
