use std::collections::{BTreeMap, BTreeSet};

use apibara_dna_protocol::starknet;
use error_stack::{Result, ResultExt};
use rkyv::Deserialize;

use crate::{segment::store, server::StreamServerError};

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
    pub fn add_event(
        &mut self,
        index: u32,
        event: &store::ArchivedEvent,
    ) -> Result<(), StreamServerError> {
        if self.events.contains_key(&index) {
            return Ok(());
        }
        let event: store::Event = event
            .deserialize(&mut rkyv::Infallible)
            .change_context(StreamServerError)?;
        let event = starknet::Event::from(event);
        self.events.insert(index, event);
        Ok(())
    }

    pub fn add_message(
        &mut self,
        index: u32,
        message: &store::ArchivedMessageToL1,
    ) -> Result<(), StreamServerError> {
        if self.messages.contains_key(&index) {
            return Ok(());
        }
        let message: store::MessageToL1 = message
            .deserialize(&mut rkyv::Infallible)
            .change_context(StreamServerError)?;
        let message = starknet::MessageToL1::from(message);
        self.messages.insert(index, message);
        Ok(())
    }

    pub fn add_transaction(
        &mut self,
        index: u32,
        transaction: &store::ArchivedTransaction,
    ) -> Result<(), StreamServerError> {
        if self.transactions.contains_key(&index) {
            return Ok(());
        }
        let transaction: store::Transaction = transaction
            .deserialize(&mut rkyv::Infallible)
            .change_context(StreamServerError)?;
        let transaction = starknet::Transaction::from(transaction);
        self.transactions.insert(index, transaction);
        Ok(())
    }

    pub fn add_receipt(
        &mut self,
        index: u32,
        receipt: &store::ArchivedTransactionReceipt,
    ) -> Result<(), StreamServerError> {
        if self.receipts.contains_key(&index) {
            return Ok(());
        }
        let receipt: store::TransactionReceipt = receipt
            .deserialize(&mut rkyv::Infallible)
            .change_context(StreamServerError)?;
        let receipt = starknet::TransactionReceipt::from(receipt);
        self.receipts.insert(index, receipt);
        Ok(())
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
