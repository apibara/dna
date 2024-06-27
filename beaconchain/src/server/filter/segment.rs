use std::ops::RangeInclusive;

use apibara_dna_common::{
    segment::{LazySegmentReader, SegmentDataOptions, SegmentGroupOptions, SegmentOptions},
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use rkyv::Deserialize;
use roaring::RoaringBitmap;
use tracing::debug;

use crate::segment::{
    store, BLOB_SEGMENT_NAME, HEADER_SEGMENT_NAME, TRANSACTION_SEGMENT_NAME, VALIDATOR_SEGMENT_NAME,
};

use super::{bag::DataBag, data::BlockData, error::SegmentFilterError, root::Filter};

pub struct SegmentFilter<S: StorageBackend + Send> {
    filters: Vec<Filter>,
    segment_options: SegmentOptions,
    segment_group_reader: LazySegmentReader<S, SegmentGroupOptions, store::SegmentGroup>,
    header_segment_reader: LazySegmentReader<S, SegmentDataOptions, store::BlockHeaderSegment>,
    validator_segment_reader: LazySegmentReader<S, SegmentDataOptions, store::ValidatorSegment>,
    transaction_segment_reader: LazySegmentReader<S, SegmentDataOptions, store::TransactionSegment>,
    blob_segment_reader: LazySegmentReader<S, SegmentDataOptions, store::BlobSegment>,
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

        let header_segment_reader = {
            let info = SegmentDataOptions(segment_options.clone(), HEADER_SEGMENT_NAME.to_string());
            LazySegmentReader::new(storage.clone(), info)
        };

        let validator_segment_reader = {
            let info =
                SegmentDataOptions(segment_options.clone(), VALIDATOR_SEGMENT_NAME.to_string());
            LazySegmentReader::new(storage.clone(), info)
        };

        let transaction_segment_reader = {
            let info = SegmentDataOptions(
                segment_options.clone(),
                TRANSACTION_SEGMENT_NAME.to_string(),
            );
            LazySegmentReader::new(storage.clone(), info)
        };

        let blob_segment_reader = {
            let info = SegmentDataOptions(segment_options.clone(), BLOB_SEGMENT_NAME.to_string());
            LazySegmentReader::new(storage.clone(), info)
        };

        let filters = filters.into_iter().map(Filter::from).collect::<Vec<_>>();

        Self {
            filters,
            segment_options,
            segment_group_reader,
            header_segment_reader,
            validator_segment_reader,
            transaction_segment_reader,
            blob_segment_reader,
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

        if !needs_linear_scan {
            let segment_group = self
                .segment_group_reader
                .read(starting_block)
                .await
                .change_context(SegmentFilterError)
                .attach_printable("failed to load segment group")?;
            let segment_group: store::SegmentGroup = segment_group
                .deserialize(&mut rkyv::Infallible)
                .change_context(SegmentFilterError)?;

            'outer: for filter in &self.filters {
                for tx_filter in filter.transactions() {
                    // Address index:
                    // If both no from and to, scan linearly.
                    // Add blocks for from/to addresses.
                    let from_address_bitmap = match &tx_filter.from {
                        None => None,
                        Some(address) => {
                            match segment_group.index.transaction_by_from_address.get(address) {
                                None => Some(RoaringBitmap::new()),
                                Some(bitmap) => {
                                    let addr_bitmap =
                                        RoaringBitmap::deserialize_from(bitmap.0.as_slice())
                                            .change_context(SegmentFilterError)
                                            .attach_printable(
                                                "failed to deserialize from address bitmap",
                                            )?;
                                    Some(addr_bitmap)
                                }
                            }
                        }
                    };

                    let to_address_bitmap = match &tx_filter.to {
                        None => None,
                        Some(address) => {
                            match segment_group.index.transaction_by_from_address.get(address) {
                                None => Some(RoaringBitmap::new()),
                                Some(bitmap) => {
                                    let addr_bitmap =
                                        RoaringBitmap::deserialize_from(bitmap.0.as_slice())
                                            .change_context(SegmentFilterError)
                                            .attach_printable(
                                                "failed to deserialize to address bitmap",
                                            )?;
                                    Some(addr_bitmap)
                                }
                            }
                        }
                    };

                    match (from_address_bitmap, to_address_bitmap) {
                        // Both addresses are present, intersect the bitmaps.
                        (Some(from_bitmap), Some(to_bitmap)) => {
                            *bitmap |= from_bitmap & to_bitmap;
                        }
                        // Only from address is present, use it.
                        (Some(from_bitmap), None) => {
                            *bitmap |= from_bitmap;
                        }
                        // Only to address is present, use it.
                        (None, Some(to_bitmap)) => {
                            *bitmap |= to_bitmap;
                        }
                        // No addresses. We need to scan linearly.
                        (None, None) => {
                            needs_linear_scan = true;
                            break 'outer;
                        }
                    }
                }
            }
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

        let should_load_validators = self.filters.iter().any(Filter::has_validators);
        if should_load_validators {
            debug!(block_number, "loading validators");
            let validator_segment = self
                .validator_segment_reader
                .read(block_number)
                .await
                .change_context(SegmentFilterError)?;
            let indexed_validators = &validator_segment.blocks[relative_index]
                .as_proposed()
                .ok_or(SegmentFilterError)
                .attach_printable("missing validator for proposed block")?;
            assert!(indexed_validators.block_number == block_number);

            let index: store::ValidatorsIndex = indexed_validators
                .index
                .deserialize(&mut rkyv::Infallible)
                .change_context(SegmentFilterError)
                .attach_printable("failed to deserialize validator index")?;
            let validators_count = indexed_validators.data.len();

            // We should move the validators iteration in the outer loop to avoid rescanning the large list
            // multiple times.
            let mut validators_to_store = RoaringBitmap::new();
            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                validators_to_store.clear();

                filter
                    .fill_validator_bitmap(validators_count, &index, &mut validators_to_store)
                    .change_context(SegmentFilterError)
                    .attach_printable("failed to fill validator bitmap")?;

                block_data.extend_validators(validators_to_store.iter());
                for validator_index in validators_to_store.iter() {
                    let validator: store::Validator = indexed_validators.data
                        [validator_index as usize]
                        .deserialize(&mut rkyv::Infallible)
                        .change_context(SegmentFilterError)
                        .attach_printable("failed to deserialize validator")?;
                    bag.add_validator(validator_index, validator);
                }
            }
        }

        let should_load_blobs = self.filters.iter().any(Filter::has_blobs);
        if should_load_blobs {
            debug!(block_number, "loading blobs");
            let blob_segment = self
                .blob_segment_reader
                .read(block_number)
                .await
                .change_context(SegmentFilterError)?;
            let blobs = &blob_segment.blocks[relative_index]
                .as_proposed()
                .ok_or(SegmentFilterError)
                .attach_printable("missing blob for proposed block")?;
            assert!(blobs.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for blob in blobs.data.iter() {
                    let blob_index = blob.blob_index;
                    let transaction_index = blob.transaction_index;

                    if let Some(extra_data) = filter.match_blob(blob) {
                        block_data.add_blob(blob_index);
                        bag.add_blob(blob_index, blob)?;

                        if extra_data.include_transaction {
                            block_data.add_transaction(transaction_index);
                            bag.defer_transaction(transaction_index);
                        }
                    }
                }
            }
        }

        let should_load_transactions =
            bag.has_deferred_transactions() || self.filters.iter().any(Filter::has_transactions);
        if should_load_transactions {
            debug!(block_number, "loading transactions");
            let transaction_segment = self
                .transaction_segment_reader
                .read(block_number)
                .await
                .change_context(SegmentFilterError)?;
            let transactions = &transaction_segment.blocks[relative_index]
                .as_proposed()
                .ok_or(SegmentFilterError)
                .attach_printable("missing transaction for proposed block")?;
            assert!(transactions.block_number == block_number);

            for (filter, block_data) in self.filters.iter().zip(blocks_data.iter_mut()) {
                for transaction in transactions.data.iter() {
                    let transaction_index = transaction.transaction_index;
                    if let Some(extra_data) = filter.match_transaction(transaction) {
                        block_data.add_transaction(transaction_index);
                        bag.add_transaction(transaction_index, transaction)?;

                        if extra_data.include_blob {
                            block_data.add_transaction_blob(transaction_index);
                            bag.defer_transaction_blob(transaction_index);
                        }
                    } else if bag.has_deferred_transaction(transaction_index) {
                        bag.add_transaction(transaction_index, transaction)?;
                    }
                }
            }
        }

        if bag.has_deferred_transaction_blobs() {
            debug!(block_number, "loading deferred blobs");
            let blob_segment = self
                .blob_segment_reader
                .read(block_number)
                .await
                .change_context(SegmentFilterError)?;
            let blobs = &blob_segment.blocks[relative_index]
                .as_proposed()
                .ok_or(SegmentFilterError)
                .attach_printable("missing blob for proposed block")?;
            assert!(blobs.block_number == block_number);

            for blob in blobs.data.iter() {
                let blob_index = blob.blob_index;
                let transaction_index = blob.transaction_index;

                if bag.has_deferred_transaction_blob(transaction_index) {
                    bag.add_blob(blob_index, blob)?;

                    for block_data in blocks_data.iter_mut() {
                        if block_data.has_transaction_blob(transaction_index) {
                            block_data.add_blob(blob_index);
                        }
                    }
                }
            }
        }

        let mut blocks = Vec::with_capacity(self.filters.len());
        let mut any_data = false;

        for block_data in blocks_data {
            if block_data.is_empty() {
                blocks.push(beaconchain::Block::default());
            }

            any_data = true;

            let validators = block_data
                .validators()
                .map(|index| {
                    bag.validator(index)
                        .ok_or(SegmentFilterError)
                        .attach_printable_lazy(|| format!("validator not found: {index}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .attach_printable("failed to collect validators")?;

            let transactions = block_data
                .transactions()
                .map(|index| {
                    bag.transaction(index)
                        .ok_or(SegmentFilterError)
                        .attach_printable_lazy(|| format!("transaction not found: {index}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .attach_printable("failed to collect transactions")?;

            let blobs = block_data
                .blobs()
                .map(|index| {
                    bag.blob(index)
                        .ok_or(SegmentFilterError)
                        .attach_printable_lazy(|| format!("blob not found: {index}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .attach_printable("failed to collect blobs")?;

            blocks.push(beaconchain::Block {
                header: Some(header.clone()),
                transactions,
                validators,
                blobs,
            });
        }

        if any_data {
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }
}
