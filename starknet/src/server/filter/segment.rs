use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeInclusive,
};

use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::starknet;
use error_stack::ResultExt;
use roaring::RoaringBitmap;
use tracing::debug;

use crate::segment::{
    reader, store, EVENT_SEGMENT_NAME, HEADER_SEGMENT_NAME, MESSAGE_SEGMENT_NAME,
    TRANSACTION_RECEIPT_SEGMENT_NAME, TRANSACTION_SEGMENT_NAME,
};

use super::{bag::DataBag, data::BlockData, root::Filter};

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
        let header_segment_reader = reader::LazySegment::new(
            storage.clone(),
            segment_options.clone(),
            HEADER_SEGMENT_NAME,
        );

        let event_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), EVENT_SEGMENT_NAME);

        let message_segment_reader = reader::LazySegment::new(
            storage.clone(),
            segment_options.clone(),
            MESSAGE_SEGMENT_NAME,
        );

        let transaction_segment_reader = reader::LazySegment::new(
            storage.clone(),
            segment_options.clone(),
            TRANSACTION_SEGMENT_NAME,
        );

        let receipt_segment_reader = reader::LazySegment::new(
            storage.clone(),
            segment_options.clone(),
            TRANSACTION_RECEIPT_SEGMENT_NAME,
        );

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
        let mut bag = DataBag::default();

        for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
            block_data.require_header(filter.has_required_header());
        }

        // Most users will filter data based on events, so we check this first.
        let should_load_events = self.filters.iter().any(Filter::has_events);
        if should_load_events {
            debug!(block_number, "loading events");
            let event_segment = self.event_segment_reader.read(block_number).await?;
            let events = &event_segment.blocks[relative_index];
            assert!(events.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for event in &events.data {
                    if let Some(extra_data) = filter.match_event(event) {
                        block_data.add_event(event.event_index);
                        bag.add_event(event.event_index, &event);

                        if extra_data.include_transaction {
                            block_data.add_transaction(event.transaction_index);
                            bag.defer_transaction(event.transaction_index);
                        }
                        if extra_data.include_receipt {
                            block_data.add_receipt(event.transaction_index);
                            bag.defer_receipt(event.transaction_index);
                        }
                        if extra_data.include_messages {
                            block_data.add_transaction_messages(event.transaction_index);
                            bag.defer_transaction_messages(event.transaction_index);
                        }
                        if extra_data.include_events {
                            block_data.add_transaction_events(event.transaction_index);
                            bag.defer_transaction_events(event.transaction_index);
                        }
                    }
                }
            }
        }

        // Load and filter messages if needed.
        let should_load_messages = self.filters.iter().any(Filter::has_messages);
        if should_load_messages {
            debug!(block_number, "loading messages");
            let message_segment = self.message_segment_reader.read(block_number).await?;
            let messages = &message_segment.blocks[relative_index];
            assert!(messages.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for message in &messages.data {
                    if let Some(extra_data) = filter.match_message(message) {
                        block_data.add_message(message.message_index);
                        bag.add_message(message.message_index, &message);

                        if extra_data.include_transaction {
                            block_data.add_transaction(message.transaction_index);
                            bag.defer_transaction(message.transaction_index);
                        }
                        if extra_data.include_receipt {
                            block_data.add_receipt(message.transaction_index);
                            bag.defer_receipt(message.transaction_index);
                        }
                        if extra_data.include_events {
                            block_data.add_transaction_events(message.transaction_index);
                            bag.defer_transaction_events(message.transaction_index)
                        }
                    }
                }
            }
        }

        let should_load_transactions =
            bag.has_deferred_transactions() || self.filters.iter().any(Filter::has_transactions);

        // Filter transactions if needed.
        if should_load_transactions {
            debug!(block_number, "loading transactions");
            let transaction_segment = self.transaction_segment_reader.read(block_number).await?;
            let transactions = &transaction_segment.blocks[relative_index];
            assert!(transactions.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for transaction in &transactions.data {
                    let transaction_index = transaction.meta().transaction_index;
                    if let Some(extra_data) = filter.match_transaction(transaction) {
                        block_data.add_transaction(transaction_index);
                        bag.add_transaction(transaction_index, &transaction);

                        if extra_data.include_receipt {
                            block_data.add_receipt(transaction_index);
                            bag.defer_receipt(transaction_index);
                        }
                        if extra_data.include_events {
                            block_data.add_transaction_events(transaction_index);
                            bag.defer_transaction_events(transaction_index);
                        }
                        if extra_data.include_messages {
                            block_data.add_transaction_messages(transaction_index);
                            bag.defer_transaction_messages(transaction_index);
                        }
                    } else if bag.has_deferred_transaction(transaction_index) {
                        bag.add_transaction(transaction_index, &transaction);
                    }
                }
            }
        }

        if bag.has_deferred_receipts() {
            debug!(block_number, "loading receipts");
            let receipt_segment = self.receipt_segment_reader.read(block_number).await?;
            let receipts = &receipt_segment.blocks[relative_index];
            assert!(receipts.block_number == block_number);

            for transaction_index in bag.deferred_receipts() {
                let receipt = &receipts.data[transaction_index as usize];
                bag.add_receipt(transaction_index, receipt);
            }
        }

        if bag.has_deferred_transaction_events() {
            debug!(block_number, "loading deferred events");
            let event_segment = self.event_segment_reader.read(block_number).await?;
            let events = &event_segment.blocks[relative_index];
            assert!(events.block_number == block_number);

            for event in &events.data {
                let transaction_index = event.transaction_index;
                if bag.has_deferred_transaction_event(transaction_index) {
                    bag.add_event(event.event_index, event);

                    for block_data in blocks_data.iter_mut() {
                        if block_data.has_transaction_events(transaction_index) {
                            block_data.add_event(event.event_index);
                        }
                    }
                }
            }
        }

        if bag.has_deferred_transaction_messages() {
            debug!(block_number, "loading deferred messages");
            let message_segment = self.message_segment_reader.read(block_number).await?;
            let messages = &message_segment.blocks[relative_index];
            assert!(messages.block_number == block_number);

            for message in &messages.data {
                let transaction_index = message.transaction_index;
                if bag.has_deferred_transaction_message(transaction_index) {
                    bag.add_message(message.message_index, message);

                    for block_data in blocks_data.iter_mut() {
                        if block_data.has_transaction_messages(transaction_index) {
                            block_data.add_message(message.message_index);
                        }
                    }
                }
            }
        }

        let mut blocks = Vec::with_capacity(self.filters.len());
        let mut any_data = false;

        for block_data in blocks_data {
            if block_data.is_empty() {
                blocks.push(starknet::Block::default());
            }

            any_data = true;
            let events = block_data
                .events()
                .map(|index| {
                    bag.event(index)
                        .ok_or(DnaError::Fatal)
                        .attach_printable_lazy(|| format!("event not found: {index}"))
                })
                .collect::<Result<Vec<_>>>()
                .attach_printable_lazy(|| {
                    format!("failed to collect events at block {block_number}")
                })?;

            let messages = block_data
                .messages()
                .map(|index| {
                    bag.message(index)
                        .ok_or(DnaError::Fatal)
                        .attach_printable_lazy(|| format!("message to L1 not found: {index}"))
                })
                .collect::<Result<Vec<_>>>()
                .attach_printable_lazy(|| {
                    format!("failed to collect messages at block {block_number}")
                })?;

            let transactions = block_data
                .transactions()
                .map(|index| {
                    bag.transaction(index)
                        .ok_or(DnaError::Fatal)
                        .attach_printable_lazy(|| format!("transaction not found: {index}"))
                })
                .collect::<Result<Vec<_>>>()
                .attach_printable_lazy(|| {
                    format!("failed to collect transactions at block {block_number}")
                })?;

            let receipts = block_data
                .receipts()
                .map(|index| {
                    bag.receipt(index)
                        .ok_or(DnaError::Fatal)
                        .attach_printable_lazy(|| format!("receipt not found: {index}"))
                })
                .collect::<Result<Vec<_>>>()
                .attach_printable_lazy(|| {
                    format!("failed to collect receipts at block {block_number}")
                })?;

            blocks.push(starknet::Block {
                header: Some(header.clone()),
                events,
                messages,
                transactions,
                receipts,
            });
        }

        if any_data {
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }
}
