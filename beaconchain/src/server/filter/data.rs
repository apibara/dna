use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct BlockData {
    header: bool,
    validators: BTreeSet<u32>,
    blobs: BTreeSet<u32>,
    transactions: BTreeSet<u32>,

    // Blobs by transaction index.
    transaction_blobs: BTreeSet<u32>,
}

impl BlockData {
    pub fn require_header(&mut self, header: bool) {
        self.header = header;
    }

    pub fn is_empty(&self) -> bool {
        (!self.header)
            && self.validators.is_empty()
            && self.blobs.is_empty()
            && self.transactions.is_empty()
    }

    pub fn extend_validators(&mut self, validators: impl Iterator<Item = u32>) {
        self.validators.extend(validators);
    }

    pub fn validators(&self) -> impl Iterator<Item = &u32> {
        self.validators.iter()
    }

    pub fn add_blob(&mut self, blob_index: u32) {
        self.blobs.insert(blob_index);
    }

    pub fn add_transaction_blob(&mut self, transaction_index: u32) {
        self.transaction_blobs.insert(transaction_index);
    }

    pub fn has_transaction_blob(&self, transaction_index: u32) -> bool {
        self.transaction_blobs.contains(&transaction_index)
    }

    pub fn blobs(&self) -> impl Iterator<Item = &u32> {
        self.blobs.iter()
    }

    pub fn add_transaction(&mut self, transaction_index: u32) {
        self.transactions.insert(transaction_index);
    }

    pub fn transactions(&self) -> impl Iterator<Item = &u32> {
        self.transactions.iter()
    }
}
