use std::collections::{BTreeMap, BTreeSet};

use apibara_dna_protocol::starknet;

use crate::segment::store;

/// Holds the data shared by one or more blocks.
#[derive(Default, Debug)]
pub struct DataBag {
    events: BTreeMap<u32, starknet::Event>,
    messages: BTreeMap<u32, starknet::MessageToL1>,
    transactions: BTreeMap<u32, starknet::Transaction>,
    receipts: BTreeMap<u32, starknet::TransactionReceipt>,

    deferred_transaction_events: BTreeSet<u32>,
    deferred_transaction_messages: BTreeSet<u32>,
    deferred_transactions: BTreeSet<u32>,
    deferred_receipts: BTreeSet<u32>,
}

impl DataBag {
    pub fn add_event(&mut self, index: u32, event: &store::Event) {
        if self.events.contains_key(&index) {
            return;
        }
        let event = starknet::Event::from(event);
        self.events.insert(index, event);
    }

    pub fn add_message(&mut self, index: u32, message: &store::MessageToL1) {
        if self.messages.contains_key(&index) {
            return;
        }
        let message = starknet::MessageToL1::from(message);
        self.messages.insert(index, message);
    }

    pub fn add_transaction(&mut self, index: u32, transaction: &store::Transaction) {
        if self.transactions.contains_key(&index) {
            return;
        }
        let transaction = starknet::Transaction::from(transaction);
        self.transactions.insert(index, transaction);
    }

    pub fn add_receipt(&mut self, index: u32, receipt: &store::TransactionReceipt) {
        if self.receipts.contains_key(&index) {
            return;
        }
        let receipt = starknet::TransactionReceipt::from(receipt);
        self.receipts.insert(index, receipt);
    }

    pub fn defer_transaction_events(&mut self, transaction_index: u32) {
        self.deferred_transaction_events.insert(transaction_index);
    }

    pub fn has_deferred_transaction_events(&self) -> bool {
        !self.deferred_transaction_events.is_empty()
    }

    pub fn has_deferred_transaction_event(&self, transaction_index: u32) -> bool {
        self.deferred_transaction_events
            .contains(&transaction_index)
    }

    pub fn defer_transaction_messages(&mut self, transaction_index: u32) {
        self.deferred_transaction_messages.insert(transaction_index);
    }

    pub fn has_deferred_transaction_messages(&self) -> bool {
        !self.deferred_transaction_messages.is_empty()
    }

    pub fn has_deferred_transaction_message(&self, transaction_index: u32) -> bool {
        self.deferred_transaction_messages
            .contains(&transaction_index)
    }

    pub fn defer_transaction(&mut self, index: u32) {
        self.deferred_transactions.insert(index);
    }

    pub fn has_deferred_transactions(&self) -> bool {
        !self.deferred_transactions.is_empty()
    }

    pub fn has_deferred_transaction(&self, transaction_index: u32) -> bool {
        self.deferred_transactions.contains(&transaction_index)
    }

    pub fn defer_receipt(&mut self, index: u32) {
        self.deferred_receipts.insert(index);
    }

    pub fn has_deferred_receipts(&self) -> bool {
        !self.deferred_receipts.is_empty()
    }

    pub fn deferred_receipts(&self) -> Vec<u32> {
        self.deferred_receipts.iter().copied().collect()
    }

    pub fn event(&self, index: &u32) -> Option<starknet::Event> {
        self.events.get(index).cloned()
    }

    pub fn message(&self, index: &u32) -> Option<starknet::MessageToL1> {
        self.messages.get(index).cloned()
    }

    pub fn transaction(&self, index: &u32) -> Option<starknet::Transaction> {
        self.transactions.get(index).cloned()
    }

    pub fn receipt(&self, index: &u32) -> Option<starknet::TransactionReceipt> {
        self.receipts.get(index).cloned()
    }
}
