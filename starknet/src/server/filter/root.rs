use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
};

use apibara_dna_common::{
    error::Result,
    segment::SegmentOptions,
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::starknet;
use roaring::RoaringBitmap;
use tracing::debug;

use crate::segment::{
    reader, store, EVENT_SEGMENT_NAME, HEADER_SEGMENT_NAME, MESSAGE_SEGMENT_NAME,
    TRANSACTION_RECEIPT_SEGMENT_NAME, TRANSACTION_SEGMENT_NAME,
};

pub struct Filter {
    header: HeaderFilter,
    events: Vec<EventFilter>,
    messages: Vec<MessageToL1Filter>,
    transactions: Vec<TransactionFilter>,
}

struct HeaderFilter {
    always: bool,
}

struct EventFilter {
    from_address: Option<store::FieldElement>,
    keys: Vec<Key>,
    strict: bool,
    include_reverted: bool,
    include_transaction: bool,
    include_receipt: bool,
    include_messages: bool,
    include_siblings: bool,
}

enum Key {
    Any,
    Exact(store::FieldElement),
}

struct MessageToL1Filter {
    to_address: Option<store::FieldElement>,
    include_reverted: bool,
    include_transaction: bool,
    include_receipt: bool,
}

struct TransactionFilter {
    include_reverted: bool,
    include_receipt: bool,
    include_events: bool,
    include_messages: bool,
    inner: Option<InnerTransactionFilter>,
}

enum InnerTransactionFilter {}

/// Additional data to include in the response.
pub struct AdditionalData {
    pub include_transaction: bool,
    pub include_receipt: bool,
    pub include_events: bool,
    pub include_messages: bool,
}

impl Filter {
    pub fn has_required_header(&self) -> bool {
        self.header.always
    }

    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    pub fn has_messages(&self) -> bool {
        !self.messages.is_empty()
    }

    pub fn has_transactions(&self) -> bool {
        !self.transactions.is_empty()
    }

    pub fn match_event(&self, event: &store::Event) -> Option<AdditionalData> {
        let mut additional_data = AdditionalData::default();
        let mut any_match = false;

        for filter in &self.events {
            if filter.matches(event) {
                any_match = true;
                additional_data.include_transaction |= filter.include_transaction;
                additional_data.include_receipt |= filter.include_receipt;
                additional_data.include_events |= filter.include_siblings;
                additional_data.include_messages |= filter.include_messages;
            }
        }

        if any_match {
            Some(additional_data)
        } else {
            None
        }
    }

    pub fn match_message(&self, message: &store::MessageToL1) -> Option<AdditionalData> {
        let mut additional_data = AdditionalData::default();
        let mut any_match = false;

        for filter in &self.messages {
            if filter.matches(message) {
                any_match = true;
                additional_data.include_transaction |= filter.include_transaction;
                additional_data.include_receipt |= filter.include_receipt;
            }
        }

        if any_match {
            Some(additional_data)
        } else {
            None
        }
    }

    pub fn match_transaction(&self, transaction: &store::Transaction) -> Option<AdditionalData> {
        let mut additional_data = AdditionalData::default();
        let mut any_match = false;

        for filter in &self.transactions {
            if filter.matches(transaction) {
                any_match = true;
                additional_data.include_receipt |= filter.include_receipt;
                additional_data.include_events |= filter.include_events;
                additional_data.include_messages |= filter.include_messages;
            }
        }

        if any_match {
            Some(additional_data)
        } else {
            None
        }
    }
}

impl Default for AdditionalData {
    fn default() -> Self {
        Self {
            include_transaction: false,
            include_receipt: false,
            include_events: false,
            include_messages: false,
        }
    }
}

impl EventFilter {
    pub fn matches(&self, event: &store::Event) -> bool {
        // If reverted, then we must include reverted events.
        if !self.include_reverted && event.transaction_reverted {
            return false;
        }

        // Address must match.
        if let Some(from_address) = &self.from_address {
            if from_address != &event.from_address {
                return false;
            }
        }

        // If strict, then length must match exactly.
        if self.strict && self.keys.len() != event.keys.len() {
            return false;
        }

        // If not strict, then all keys must be present.
        if self.keys.len() > event.keys.len() {
            return false;
        }

        // Compare event keys.
        for (key, event_key) in self.keys.iter().zip(&event.keys) {
            match key {
                Key::Any => continue,
                Key::Exact(expected) => {
                    if expected != event_key {
                        return false;
                    }
                }
            }
        }

        true
    }
}

impl MessageToL1Filter {
    pub fn matches(&self, message: &store::MessageToL1) -> bool {
        // If reverted, then we must include reverted messages.
        if !self.include_reverted && message.transaction_reverted {
            return false;
        }

        let Some(to_address) = &self.to_address else {
            return true;
        };

        to_address == &message.to_address
    }
}

impl TransactionFilter {
    pub fn matches(&self, transaction: &store::Transaction) -> bool {
        let meta = transaction.meta();
        // If reverted, then we must include reverted transactions.
        if !self.include_reverted && meta.transaction_reverted {
            return false;
        }

        // If it's an empty filter, then we match everything.
        let Some(inner) = &self.inner else {
            return true;
        };

        todo!()
    }
}

impl From<starknet::Filter> for Filter {
    fn from(filter: starknet::Filter) -> Self {
        let header = filter.header.map(HeaderFilter::from).unwrap_or_default();
        let events = filter.events.into_iter().map(EventFilter::from).collect();
        let messages = filter
            .messages
            .into_iter()
            .map(MessageToL1Filter::from)
            .collect();
        let transactions = filter
            .transactions
            .into_iter()
            .map(TransactionFilter::from)
            .collect();

        Self {
            header,
            events,
            messages,
            transactions,
        }
    }
}

impl From<starknet::HeaderFilter> for HeaderFilter {
    fn from(filter: starknet::HeaderFilter) -> Self {
        Self {
            always: filter.always.unwrap_or(false),
        }
    }
}

impl From<starknet::EventFilter> for EventFilter {
    fn from(value: starknet::EventFilter) -> Self {
        let from_address = value.from_address.map(store::FieldElement::from);
        let keys = value.keys.into_iter().map(Key::from).collect();
        let strict = value.strict.unwrap_or(false);
        let include_reverted = value.include_reverted.unwrap_or(false);
        let include_transaction = value.include_transaction.unwrap_or(false);
        let include_receipt = value.include_receipt.unwrap_or(false);
        let include_messages = value.include_messages.unwrap_or(false);
        let include_siblings = value.include_siblings.unwrap_or(false);

        EventFilter {
            from_address,
            keys,
            strict,
            include_reverted,
            include_transaction,
            include_receipt,
            include_messages,
            include_siblings,
        }
    }
}

impl From<starknet::Key> for Key {
    fn from(key: starknet::Key) -> Self {
        match key.value {
            None => Key::Any,
            Some(value) => Key::Exact(store::FieldElement::from(value)),
        }
    }
}

impl From<starknet::MessageToL1Filter> for MessageToL1Filter {
    fn from(value: starknet::MessageToL1Filter) -> Self {
        let to_address = value.to_address.map(store::FieldElement::from);

        Self {
            to_address,
            include_reverted: value.include_reverted.unwrap_or(false),
            include_transaction: value.include_transaction.unwrap_or(false),
            include_receipt: value.include_receipt.unwrap_or(false),
        }
    }
}

impl From<starknet::TransactionFilter> for TransactionFilter {
    fn from(value: starknet::TransactionFilter) -> Self {
        TransactionFilter {
            include_reverted: value.include_reverted.unwrap_or(false),
            include_receipt: value.include_receipt.unwrap_or(false),
            include_events: value.include_events.unwrap_or(false),
            include_messages: value.include_messages.unwrap_or(false),
            inner: value.inner.map(InnerTransactionFilter::from),
        }
    }
}

impl From<starknet::transaction_filter::Inner> for InnerTransactionFilter {
    fn from(value: starknet::transaction_filter::Inner) -> Self {
        todo!()
    }
}

impl Default for HeaderFilter {
    fn default() -> Self {
        Self { always: false }
    }
}
