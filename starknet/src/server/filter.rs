use std::ops::RangeInclusive;

use apibara_dna_common::{
    error::Result,
    segment::SegmentOptions,
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::starknet;
use roaring::RoaringBitmap;

use crate::segment::{reader, store};

pub struct SegmentFilter<S: StorageBackend + Send> {
    filters: Vec<starknet::Filter>,
    segment_options: SegmentOptions,
    header_segment_reader: reader::LazySegment<S, store::BlockHeaderSegment>,
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

        Self {
            filters,
            segment_options,
            header_segment_reader,
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

        let mut blocks = Vec::with_capacity(self.filters.len());

        for filter in &self.filters {
            blocks.push(starknet::Block {
                header: Some(header.into()),
                ..Default::default()
            });
        }

        Ok(Some(blocks))
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
