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
use tracing::{debug, event, span, Instrument};

use crate::segment::{reader, store};

pub struct SegmentFilter<S: StorageBackend + Send> {
    filters: Vec<starknet::Filter>,
    segment_options: SegmentOptions,
    header_segment_reader: reader::LazySegment<S, store::BlockHeaderSegment>,
    event_segment_reader: reader::LazySegment<S, store::EventSegment>,
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

        let transaction_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "transaction");

        let receipt_segment_reader =
            reader::LazySegment::new(storage.clone(), segment_options.clone(), "receipt");

        Self {
            filters,
            segment_options,
            header_segment_reader,
            event_segment_reader,
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
        let mut transactions_by_index = BTreeMap::new();

        // Most users will filter data based on events, so we check this first.
        let should_load_events = self.filters.iter().any(FilterExt::has_events);
        if should_load_events {
            debug!(block_number, "loading events");
            let event_segment = self.event_segment_reader.read(block_number).await?;
            let events = &event_segment.blocks[relative_index];
            assert!(events.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for event in &events.data {
                    block_data.add_event(event.event_index);
                    if !events_by_index.contains_key(&event.event_index) {
                        let event_proto = starknet::Event::from(event);
                        events_by_index.insert(event.event_index, event_proto);
                    }
                }
            }
        }

        let should_load_transactions = blocks_data.iter().any(BlockData::has_transactions)
            || self.filters.iter().any(FilterExt::has_transactions);

        if should_load_transactions {
            debug!(block_number, "loading transactions");
            let transaction_segment = self.transaction_segment_reader.read(block_number).await?;
            let transactions = &transaction_segment.blocks[relative_index];
            assert!(transactions.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for transaction in &transactions.data {
                    let transaction_index = transaction.meta().transaction_index;
                    block_data.add_transaction(transaction_index);
                    if !transactions_by_index.contains_key(&transaction_index) {
                        let transaction_proto = starknet::Transaction::from(transaction);
                        transactions_by_index.insert(transaction_index, transaction_proto);
                    }
                }
            }
        }

        let mut blocks = Vec::with_capacity(self.filters.len());
        for block_data in blocks_data {
            let events = block_data
                .events()
                .map(|event_index| {
                    let event = events_by_index.get(event_index).unwrap();
                    event.clone()
                })
                .collect::<Vec<_>>();

            blocks.push(starknet::Block {
                header: Some(header.clone()),
                events,
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
    receipts: BTreeSet<u32>,
}

impl BlockData {
    pub fn add_transaction(&mut self, index: u32) {
        self.transactions.insert(index);
    }

    pub fn add_event(&mut self, index: u32) {
        self.events.insert(index);
    }

    pub fn add_receipt(&mut self, index: u32) {
        self.receipts.insert(index);
    }

    pub fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    pub fn events(&self) -> impl Iterator<Item = &u32> {
        self.events.iter()
    }

    pub fn has_transactions(&self) -> bool {
        !self.transactions.is_empty()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &u32> {
        self.transactions.iter()
    }
}

trait FilterExt {
    fn has_required_header(&self) -> bool;
    fn has_transactions(&self) -> bool;
    fn has_events(&self) -> bool;
    fn has_messages(&self) -> bool;
    fn transactions(&self) -> impl Iterator<Item = &starknet::TransactionFilter>;
    fn events(&self) -> impl Iterator<Item = &starknet::EventFilter>;
}

impl FilterExt for starknet::Filter {
    fn has_required_header(&self) -> bool {
        self.header.as_ref().and_then(|h| h.always).unwrap_or(false)
    }

    fn has_transactions(&self) -> bool {
        !self.transactions.is_empty()
    }

    fn has_events(&self) -> bool {
        !self.events.is_empty()
    }

    fn has_messages(&self) -> bool {
        !self.messages.is_empty()
    }

    fn transactions(&self) -> impl Iterator<Item = &starknet::TransactionFilter> {
        self.transactions.iter()
    }

    fn events(&self) -> impl Iterator<Item = &starknet::EventFilter> {
        self.events.iter()
    }
}
