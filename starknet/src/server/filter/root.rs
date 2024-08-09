use apibara_dna_protocol::starknet;

use crate::segment::store;

use super::{event::EventFilter, message::MessageToL1Filter, transaction::TransactionFilter};

pub struct Filter {
    header: HeaderFilter,
    events: Vec<EventFilter>,
    messages: Vec<MessageToL1Filter>,
    transactions: Vec<TransactionFilter>,
}

#[derive(Default)]
pub struct HeaderFilter {
    always: bool,
}

/// Additional data to include in the response.
#[derive(Default)]
pub struct AdditionalData {
    pub include_transaction: bool,
    pub include_receipt: bool,
    pub include_events: bool,
    pub include_messages: bool,
}

impl Filter {
    pub fn needs_linear_scan(&self) -> bool {
        self.has_required_header() || self.has_transactions() || self.has_messages()
    }

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

    pub fn events(&self) -> impl Iterator<Item = &EventFilter> {
        self.events.iter()
    }

    pub fn match_event(&self, event: &store::ArchivedEvent) -> Option<AdditionalData> {
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

    pub fn match_message(&self, message: &store::ArchivedMessageToL1) -> Option<AdditionalData> {
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

    pub fn match_transaction(
        &self,
        transaction: &store::ArchivedTransaction,
    ) -> Option<AdditionalData> {
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
