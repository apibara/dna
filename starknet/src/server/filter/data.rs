use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct BlockData {
    header: bool,
    transactions: BTreeSet<u32>,
    events: BTreeSet<u32>,
    messages: BTreeSet<u32>,
    receipts: BTreeSet<u32>,

    // Events by transaction index.
    transaction_events: BTreeSet<u32>,
    // Messages by transaction index.
    transaction_messages: BTreeSet<u32>,
}

impl BlockData {
    pub fn require_header(&mut self, header: bool) {
        self.header = header;
    }

    pub fn add_transaction(&mut self, index: u32) {
        self.transactions.insert(index);
    }

    pub fn add_event(&mut self, index: u32) {
        self.events.insert(index);
    }

    pub fn add_message(&mut self, index: u32) {
        self.messages.insert(index);
    }

    pub fn add_receipt(&mut self, index: u32) {
        self.receipts.insert(index);
    }

    pub fn add_transaction_events(&mut self, transaction_index: u32) {
        self.transaction_events.insert(transaction_index);
    }

    pub fn has_transaction_events(&self, transaction_index: u32) -> bool {
        self.transaction_events.contains(&transaction_index)
    }

    pub fn add_transaction_messages(&mut self, transaction_index: u32) {
        self.transaction_messages.insert(transaction_index);
    }

    pub fn has_transaction_messages(&self, transaction_index: u32) -> bool {
        self.transaction_messages.contains(&transaction_index)
    }

    pub fn events(&self) -> impl Iterator<Item = &u32> {
        self.events.iter()
    }

    pub fn messages(&self) -> impl Iterator<Item = &u32> {
        self.messages.iter()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &u32> {
        self.transactions.iter()
    }

    pub fn receipts(&self) -> impl Iterator<Item = &u32> {
        self.receipts.iter()
    }

    pub fn is_empty(&self) -> bool {
        (!self.header)
            && self.transactions.is_empty()
            && self.events.is_empty()
            && self.messages.is_empty()
            && self.receipts.is_empty()
    }
}
