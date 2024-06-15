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

use crate::segment::{reader, store};

pub struct SegmentFilter<S: StorageBackend + Send> {
    filters: Vec<Filter>,
    segment_options: SegmentOptions,
    header_segment_reader: reader::LazySegment<S, store::BlockHeaderSegment>,
    event_segment_reader: reader::LazySegment<S, store::EventSegment>,
    message_segment_reader: reader::LazySegment<S, store::MessageSegment>,
    transaction_segment_reader: reader::LazySegment<S, store::TransactionSegment>,
    receipt_segment_reader: reader::LazySegment<S, store::TransactionReceiptSegment>,
}

impl<S> SegmentFilter<S>
where
    S: StorageBackend + Send + Clone,
    S::Reader: Unpin + Send,
{
    pub fn new(
        filters: Vec<starknet::Filter>,
        storage: CachedStorage<S>,
        local_storage: LocalStorageBackend,
        segment_options: SegmentOptions,
    ) -> Self {
        let header_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "header");

        let event_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "events");

        let message_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "messages");

        let transaction_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "transaction");

        let receipt_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "receipt");

        let filters = filters.into_iter().map(Filter::from).collect::<Vec<_>>();

        Self {
            filters,
            segment_options,
            header_segment_reader,
            event_segment_reader,
            message_segment_reader,
            transaction_segment_reader,
            receipt_segment_reader,
        }
    }

    pub fn filter_len(&self) -> usize {
        self.filters.len()
    }

    #[tracing::instrument(skip_all, err(Debug))]
    pub async fn fill_block_bitmap(
        &mut self,
        bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) -> Result<()> {
        bitmap.clear();

        for filter in &self.filters {
            if filter.has_required_header() || filter.has_transactions() {
                bitmap.insert_range(block_range);
                return Ok(());
            }
        }

        // TODO: load segment group to use indices.

        todo!();
    }

    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn filter_segment_block(
        &mut self,
        block_number: u64,
    ) -> Result<Option<Vec<starknet::Block>>> {
        let segment_start = self.segment_options.segment_start(block_number);
        let relative_index = (block_number - segment_start) as usize;

        let header_segment = self.header_segment_reader.read(block_number).await?;
        let header = &header_segment.blocks[relative_index];
        let header = starknet::BlockHeader::from(header);

        let mut blocks_data = vec![BlockData::default(); self.filters.len()];
        let mut events_by_index = BTreeMap::new();
        let mut messages_by_index = BTreeMap::new();
        let mut transactions_by_index = BTreeMap::new();
        let mut receipts_by_index = BTreeMap::new();

        let mut missing_transactions = BTreeSet::new();
        let mut missing_receipts = BTreeSet::new();
        let mut missing_events_by_transaction = BTreeSet::new();
        let mut missing_messages_by_transaction = BTreeSet::new();

        // Most users will filter data based on events, so we check this first.
        let should_load_events = self.filters.iter().any(Filter::has_events);
        if should_load_events {
            debug!(block_number, "loading events");
            let event_segment = self.event_segment_reader.read(block_number).await?;
            let events = &event_segment.blocks[relative_index];
            assert!(events.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for event in &events.data {
                    if let Some(match_) = filter.match_event(event) {
                        block_data.add_event(event.event_index);
                        if !events_by_index.contains_key(&event.event_index) {
                            let event_proto = starknet::Event::from(event);
                            events_by_index.insert(event.event_index, event_proto);
                        }
                        if match_.include_transaction {
                            block_data.add_transaction(event.transaction_index);
                            missing_transactions.insert(event.transaction_index);
                        }
                        if match_.include_receipt {
                            block_data.add_receipt(event.transaction_index);
                            missing_receipts.insert(event.transaction_index);
                        }
                    }
                }
            }
        }

        // Load and filter messages if needed.
        let should_load_messages = self.filters.iter().any(Filter::has_messages);
        if should_load_messages {
            debug!(block_number, "loading events");
            let message_segment = self.message_segment_reader.read(block_number).await?;
            let messages = &message_segment.blocks[relative_index];
            assert!(messages.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for message in &messages.data {
                    if let Some(match_) = filter.match_message(message) {
                        block_data.add_message(message.message_index);
                        if !messages_by_index.contains_key(&message.message_index) {
                            let message_proto = starknet::MessageToL1::from(message);
                            messages_by_index.insert(message.message_index, message_proto);
                        }
                        if match_.include_transaction {
                            block_data.add_transaction(message.transaction_index);
                            missing_transactions.insert(message.transaction_index);
                        }
                        if match_.include_receipt {
                            block_data.add_receipt(message.transaction_index);
                            missing_receipts.insert(message.transaction_index);
                        }
                    }
                }
            }
        }

        let should_load_transactions =
            !missing_transactions.is_empty() || self.filters.iter().any(Filter::has_transactions);

        // Filter transactions if needed.
        if should_load_transactions {
            debug!(block_number, "loading transactions");
            let transaction_segment = self.transaction_segment_reader.read(block_number).await?;
            let transactions = &transaction_segment.blocks[relative_index];
            assert!(transactions.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for transaction in &transactions.data {
                    let transaction_index = transaction.meta().transaction_index;
                    if let Some(match_) = filter.match_transaction(transaction) {
                        block_data.add_transaction(transaction_index);
                        if !transactions_by_index.contains_key(&transaction_index) {
                            let transaction_proto = starknet::Transaction::from(transaction);
                            transactions_by_index.insert(transaction_index, transaction_proto);
                        }
                        if match_.include_receipt {
                            missing_receipts.insert(transaction_index);
                        }
                        if match_.include_events {
                            missing_events_by_transaction.insert(transaction_index);
                        }
                        if match_.include_messages {
                            missing_messages_by_transaction.insert(transaction_index);
                        }
                    } else if missing_transactions.contains(&transaction_index) {
                        block_data.add_transaction(transaction_index);
                        if !transactions_by_index.contains_key(&transaction_index) {
                            let transaction_proto = starknet::Transaction::from(transaction);
                            transactions_by_index.insert(transaction_index, transaction_proto);
                        }
                    }
                }
            }
        }

        if !missing_receipts.is_empty() {
            debug!(block_number, "loading receipts");
            let receipt_segment = self.receipt_segment_reader.read(block_number).await?;
            let receipts = &receipt_segment.blocks[relative_index];
            assert!(receipts.block_number == block_number);

            for transaction_index in missing_receipts.into_iter() {
                let receipt = &receipts.data[transaction_index as usize];
                let receipt_proto = starknet::TransactionReceipt::from(receipt);
                receipts_by_index.insert(transaction_index, receipt_proto);
            }
        }

        // TODO: load missing events and messages by their tx idx.
        // It's tricky because we need to know which filter asked
        // to include the extra events.

        let mut blocks = Vec::with_capacity(self.filters.len());
        for block_data in blocks_data {
            let events = block_data
                .events()
                .map(|event_index| events_by_index.get(event_index).unwrap().clone())
                .collect::<Vec<_>>();

            let messages = block_data
                .messages()
                .map(|index| messages_by_index.get(index).unwrap().clone())
                .collect::<Vec<_>>();

            let transactions = block_data
                .transactions()
                .map(|index| transactions_by_index.get(index).unwrap().clone())
                .collect::<Vec<_>>();

            let receipts = block_data
                .receipts()
                .map(|index| receipts_by_index.get(index).unwrap().clone())
                .collect::<Vec<_>>();

            blocks.push(starknet::Block {
                header: Some(header.clone()),
                events,
                messages,
                transactions,
                receipts,
                ..Default::default()
            });
        }

        Ok(Some(blocks))
    }
}

#[derive(Debug, Default, Clone)]
struct BlockData {
    transactions: BTreeSet<u32>,
    events: BTreeSet<u32>,
    messages: BTreeSet<u32>,
    receipts: BTreeSet<u32>,
}

impl BlockData {
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

    pub fn events(&self) -> impl Iterator<Item = &u32> {
        self.events.iter()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &u32> {
        self.transactions.iter()
    }

    pub fn messages(&self) -> impl Iterator<Item = &u32> {
        self.messages.iter()
    }

    pub fn receipts(&self) -> impl Iterator<Item = &u32> {
        self.receipts.iter()
    }
}

struct Filter {
    pub header: HeaderFilter,
    pub events: Vec<EventFilter>,
    pub messages: Vec<MessageToL1Filter>,
    pub transactions: Vec<TransactionFilter>,
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

struct EventMatch {
    include_transaction: bool,
    include_receipt: bool,
}

struct TransactionMatch {
    include_receipt: bool,
    include_events: bool,
    include_messages: bool,
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

    pub fn match_event(&self, event: &store::Event) -> Option<EventMatch> {
        let mut any_match = false;
        let mut include_transaction = false;
        let mut include_receipt = false;

        for filter in &self.events {
            if filter.matches(event) {
                any_match = true;
                include_transaction |= filter.include_transaction;
                include_receipt |= filter.include_receipt;
            }
        }

        if any_match {
            Some(EventMatch {
                include_transaction,
                include_receipt,
            })
        } else {
            None
        }
    }

    pub fn match_message(&self, message: &store::MessageToL1) -> Option<EventMatch> {
        let mut any_match = false;
        let mut include_transaction = false;
        let mut include_receipt = false;

        for filter in &self.messages {
            if filter.matches(message) {
                any_match = true;
                include_transaction |= filter.include_transaction;
                include_receipt |= filter.include_receipt;
            }
        }

        if any_match {
            Some(EventMatch {
                include_transaction,
                include_receipt,
            })
        } else {
            None
        }
    }

    pub fn match_transaction(&self, transaction: &store::Transaction) -> Option<TransactionMatch> {
        let mut any_match = false;
        let mut include_receipt = false;
        let mut include_events = false;
        let mut include_messages = false;

        for filter in &self.transactions {
            if filter.matches(transaction) {
                any_match = true;
                include_receipt |= filter.include_receipt;
                include_events |= filter.include_events;
                include_messages |= filter.include_messages;
            }
        }

        if any_match {
            Some(TransactionMatch {
                include_receipt,
                include_events,
                include_messages,
            })
        } else {
            None
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

        EventFilter {
            from_address,
            keys,
            strict,
            include_reverted,
            include_transaction,
            include_receipt,
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
