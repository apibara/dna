use std::ops::RangeInclusive;

use apibara_dna_common::{
    segment::{self, LazySegmentReader, SegmentDataOptions, SegmentGroupOptions, SegmentOptions},
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use rkyv::Deserialize;
use roaring::RoaringBitmap;
use tracing::debug;

use crate::segment::{store, HEADER_SEGMENT_NAME};

use super::{bag::DataBag, data::BlockData, root::Filter};

#[derive(Debug)]
pub struct SegmentFilterError;

pub struct SegmentFilter<S: StorageBackend + Send> {
    filters: Vec<Filter>,
    segment_options: SegmentOptions,
    segment_group_reader: LazySegmentReader<S, SegmentGroupOptions, store::SegmentGroup>,
    header_segment_reader: LazySegmentReader<S, SegmentDataOptions, store::BlockHeaderSegment>,
}

impl<S> SegmentFilter<S>
where
    S: StorageBackend + Send + Clone,
    S::Reader: Unpin + Send,
{
    pub fn new(
        filters: Vec<beaconchain::Filter>,
        storage: CachedStorage<S>,
        _local_storage: LocalStorageBackend,
        segment_options: SegmentOptions,
    ) -> Self {
        let segment_group_info = SegmentGroupOptions(segment_options.clone());

        let segment_group_reader = LazySegmentReader::new(storage.clone(), segment_group_info);

        let header_segment_data_info =
            SegmentDataOptions(segment_options.clone(), HEADER_SEGMENT_NAME.to_string());
        let header_segment_reader =
            LazySegmentReader::new(storage.clone(), header_segment_data_info);

        let filters = filters.into_iter().map(Filter::from).collect::<Vec<_>>();

        Self {
            filters,
            segment_options,
            segment_group_reader,
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
        starting_block: u64,
        block_range: RangeInclusive<u32>,
    ) -> Result<(), SegmentFilterError> {
        bitmap.clear();

        let mut needs_linear_scan = false;

        for filter in &self.filters {
            needs_linear_scan |= filter.needs_linear_scan();
        }

        if needs_linear_scan {
            bitmap.insert_range(block_range);
        }

        debug!(bitmap = ?bitmap, "filled block bitmap");

        Ok(())
    }

    #[tracing::instrument(skip(self), err(Debug))]
    pub async fn filter_segment_block(
        &mut self,
        block_number: u64,
    ) -> Result<Option<Vec<beaconchain::Block>>, SegmentFilterError> {
        let segment_start = self.segment_options.segment_start(block_number);
        let relative_index = (block_number - segment_start) as usize;

        let header_segment = self
            .header_segment_reader
            .read(block_number)
            .await
            .change_context(SegmentFilterError)?;

        let mut blocks_data = vec![BlockData::default(); self.filters.len()];
        let mut bag = DataBag::default();

        // Don't send any data for missed slots.
        let header = &header_segment.blocks[relative_index];
        let header = match header {
            store::ArchivedSlot::Missed => {
                debug!(block_number, "skipping missed block");
                return Ok(None);
            }
            store::ArchivedSlot::Proposed(header) => header,
        };

        let header: store::BlockHeader = header
            .deserialize(&mut rkyv::Infallible)
            .change_context(SegmentFilterError)?;
        let header = beaconchain::BlockHeader::from(header);

        for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
            block_data.require_header(filter.has_required_header());
        }

        let mut blocks = Vec::with_capacity(self.filters.len());
        let mut any_data = false;
        for block_data in blocks_data {
            if block_data.is_empty() {
                blocks.push(beaconchain::Block::default());
            }

            any_data = true;

            blocks.push(beaconchain::Block {
                header: Some(header.clone()),
                blobs: vec![],
                transactions: vec![],
                validators: vec![],
            });
        }

        if any_data {
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }
}

impl error_stack::Context for SegmentFilterError {}

impl std::fmt::Display for SegmentFilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to filter segment data")
    }
}
