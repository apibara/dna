mod filter;
mod proto_conversion;

use std::ops::RangeInclusive;

use apibara_dna_common::{
    block_store::BlockStoreReader,
    data_stream::{Scanner, ScannerAction, ScannerError, ScannerFactory, SegmentBlock, SendData},
    store::{group::SegmentGroup, segment::IndexSegment},
    Cursor,
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use filter::{Filter, FilteredDataExt};
use prost::Message;
use roaring::RoaringBitmap;
use tokio::task::JoinSet;
use tracing::{debug, field, warn};

use crate::{
    segment::{BlobSegment, HeaderSegment, TransactionSegment, ValidatorSegment},
    store::{
        self,
        fragment::{self, ArchivedSlot},
    },
};

const MAX_FILTERS_LEN: usize = 5;

pub struct BeaconChainScannerFactory {
    store: BlockStoreReader,
}

pub struct BeaconChainScanner {
    filters: Vec<Filter>,
    store: BlockStoreReader,
}

impl BeaconChainScannerFactory {
    pub fn new(store: BlockStoreReader) -> Self {
        Self { store }
    }
}

impl ScannerFactory for BeaconChainScannerFactory {
    type Scanner = BeaconChainScanner;

    fn create_scanner(&self, filters: &[Vec<u8>]) -> tonic::Result<Self::Scanner, tonic::Status> {
        let filters = filters
            .iter()
            .map(|bytes| {
                let proto = beaconchain::Filter::decode(bytes.as_slice())?;
                Ok(Filter::from(proto))
            })
            .collect::<std::result::Result<Vec<Filter>, prost::DecodeError>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        if filters.is_empty() {
            return Err(tonic::Status::invalid_argument("no filters provided"));
        }

        if filters.len() > MAX_FILTERS_LEN {
            return Err(tonic::Status::invalid_argument(format!(
                "too many filters ({} > {})",
                filters.len(),
                MAX_FILTERS_LEN
            )));
        }

        for (i, filter) in filters.iter().enumerate() {
            if filter.is_null() {
                return Err(tonic::Status::invalid_argument(format!(
                    "filter {i} will never match any data"
                )));
            }
        }

        Ok(BeaconChainScanner {
            filters,
            store: self.store.clone(),
        })
    }
}

impl Scanner for BeaconChainScanner {
    fn prefetch_segment(
        &self,
        join_set: &mut JoinSet<Result<(), ScannerError>>,
        segment_cursor: Cursor,
    ) -> Result<(), ScannerError> {
        let store = self.store.clone();
        join_set.spawn({
            let store = store.clone();
            let segment_cursor = segment_cursor.clone();
            async move {
                store
                    .get_index_segment(&segment_cursor)
                    .await
                    .change_context(ScannerError)?;
                Ok(())
            }
        });

        for segment in ["header", "transaction", "validator", "blob"] {
            join_set.spawn({
                let store = store.clone();
                let segment_cursor = segment_cursor.clone();
                async move {
                    store
                        .get_segment(&segment_cursor, segment)
                        .await
                        .change_context(ScannerError)?;
                    Ok(())
                }
            });
        }

        Ok(())
    }

    async fn fill_block_bitmap(
        &mut self,
        group_cursor: Cursor,
        blocks_in_group: usize,
        bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) -> Result<(), ScannerError> {
        debug!("filling block bitmap");

        let group_bytes = self
            .store
            .get_group(&group_cursor)
            .await
            .change_context(ScannerError)?;

        let group = rkyv::access::<rkyv::Archived<SegmentGroup>, rkyv::rancor::Error>(&group_bytes)
            .change_context(ScannerError)?;

        if group.index.tags.is_empty() {
            warn!("group is empty. this should not happen.");
            return Ok(());
        }

        let filtered = self
            .filters
            .iter()
            .map(|f| f.filter_data(&group.index))
            .collect::<Result<Vec<_>, _>>()?;

        for mut filter in filtered {
            if filter.header {
                bitmap.insert_range(block_range);
                break;
            }

            if !filter.transactions_by_blob.is_empty() {
                bitmap.insert_range(block_range);
                break;
            }

            if !filter.blobs_by_transaction.is_empty() {
                bitmap.insert_range(block_range);
                break;
            }

            for block in filter
                .transactions
                .take_matches_with_range(group_cursor.number as u32, blocks_in_group)
            {
                bitmap.insert(block.index);
            }

            for block in filter
                .validators
                .take_matches_with_range(group_cursor.number as u32, blocks_in_group)
            {
                bitmap.insert(block.index);
            }

            for block in filter
                .blobs
                .take_matches_with_range(group_cursor.number as u32, blocks_in_group)
            {
                bitmap.insert(block.index);
            }
        }

        Ok(())
    }

    async fn scan_segment<S, SR>(
        &mut self,
        segment_cursor: Cursor,
        blocks: Vec<SegmentBlock>,
        cb: S,
    ) -> Result<(), ScannerError>
    where
        S: Fn(SendData) -> SR + Send,
        SR: futures::Future<Output = ScannerAction> + Send,
    {
        debug!("scanning segment");

        // TODO: avoid loading segments if they're not needed by the filters.
        // TODO: refactor scanner to share logic between single and segment.

        let index_segment = self
            .store
            .get_index_segment(&segment_cursor)
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get index segment")?;

        let index =
            rkyv::access::<rkyv::Archived<IndexSegment>, rkyv::rancor::Error>(&index_segment)
                .change_context(ScannerError)
                .attach_printable("failed to deserialize index segment")?;

        let header_segment = self
            .store
            .get_segment(&segment_cursor, "header")
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get header segment")?;
        let header_segment =
            rkyv::access::<rkyv::Archived<HeaderSegment>, rkyv::rancor::Error>(&header_segment)
                .change_context(ScannerError)
                .attach_printable("failed to deserialize header segment")?;

        let transaction_segment = self
            .store
            .get_segment(&segment_cursor, "transaction")
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get transaction segment")?;
        let transaction_segment = rkyv::access::<
            rkyv::Archived<TransactionSegment>,
            rkyv::rancor::Error,
        >(&transaction_segment)
        .change_context(ScannerError)
        .attach_printable("failed to deserialize transaction segment")?;

        let validator_segment = self
            .store
            .get_segment(&segment_cursor, "validator")
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get validator segment")?;
        let validator_segment =
            rkyv::access::<rkyv::Archived<ValidatorSegment>, rkyv::rancor::Error>(
                &validator_segment,
            )
            .change_context(ScannerError)
            .attach_printable("failed to deserialize validator segment")?;

        let blob_segment = self
            .store
            .get_segment(&segment_cursor, "blob")
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get blob segment")?;
        let blob_segment =
            rkyv::access::<rkyv::Archived<BlobSegment>, rkyv::rancor::Error>(&blob_segment)
                .change_context(ScannerError)
                .attach_printable("failed to deserialize blob segment")?;

        for block in blocks {
            let span = tracing::info_span!(
                "stream_send_block",
                cursor = %block.end_cursor,
                data_size = field::Empty,
                blocks_count = field::Empty,
                transactions_count = field::Empty,
                validators_count = field::Empty,
                blobs_count = field::Empty
            );

            debug!(block = ?block, "scanning segment block");
            let block_index = &index.blocks[block.offset];

            // Missed slots have no data and no indices.
            if block_index.data.tags.is_empty() {
                continue;
            }

            let filtered = self
                .filters
                .iter()
                .map(|f| f.filter_data(&block_index.data))
                .collect::<Result<Vec<_>, _>>()?;

            if filtered.has_no_data() {
                continue;
            }

            let ArchivedSlot::Proposed(block_header) = &header_segment.blocks[block.offset].data
            else {
                return Err(ScannerError)
                    .attach_printable("header segment data is not a block")
                    .attach_printable_lazy(|| format!("block: {block:?}"));
            };

            let ArchivedSlot::Proposed(block_transactions) =
                &transaction_segment.blocks[block.offset].data
            else {
                return Err(ScannerError)
                    .attach_printable("transaction segment data is not a block")
                    .attach_printable_lazy(|| format!("block: {block:?}"));
            };

            let ArchivedSlot::Proposed(block_validators) =
                &validator_segment.blocks[block.offset].data
            else {
                return Err(ScannerError)
                    .attach_printable("validator segment data is not a block")
                    .attach_printable_lazy(|| format!("block: {block:?}"));
            };

            let ArchivedSlot::Proposed(block_blobs) = &blob_segment.blocks[block.offset].data
            else {
                return Err(ScannerError)
                    .attach_printable("blob segment data is not a block")
                    .attach_printable_lazy(|| format!("block: {block:?}"));
            };

            let mut encoded = Vec::with_capacity(filtered.len());

            let mut transactions_count = 0;
            let mut validators_count = 0;
            let mut blobs_count = 0;

            for mut data in filtered {
                let mut result = beaconchain::Block::default();

                let header: beaconchain::BlockHeader = block_header.into();
                result.header = Some(header);

                for data_ref in data
                    .transactions_by_blob
                    .take_matches(block_transactions.len())
                {
                    for blob in block_blobs.iter() {
                        if blob.transaction_index == data_ref.index {
                            data.transactions.add_data_ref(data_ref);
                            break;
                        }
                    }
                }

                for data_ref in data.blobs_by_transaction.take_matches(block_blobs.len()) {
                    for transaction in block_transactions.iter() {
                        if transaction.transaction_index == data_ref.index {
                            data.blobs.add_data_ref(data_ref);
                            break;
                        }
                    }
                }

                for data_ref in data.transactions.take_matches(block_transactions.len()) {
                    let mut transaction: beaconchain::Transaction =
                        (&block_transactions[data_ref.index as usize]).into();
                    transaction.filter_ids = data_ref.filter_ids.into_iter().collect();
                    result.transactions.push(transaction);
                }

                for data_ref in data.validators.take_matches(block_validators.len()) {
                    let mut validator: beaconchain::Validator =
                        (&block_validators[data_ref.index as usize]).into();
                    validator.filter_ids = data_ref.filter_ids.into_iter().collect();
                    result.validators.push(validator);
                }

                for data_ref in data.blobs.take_matches(block_blobs.len()) {
                    let mut blob: beaconchain::Blob =
                        (&block_blobs[data_ref.index as usize]).into();
                    blob.filter_ids = data_ref.filter_ids.into_iter().collect();
                    result.blobs.push(blob);
                }

                transactions_count += result.transactions.len();
                validators_count += result.validators.len();
                blobs_count += result.blobs.len();

                encoded.push(result.encode_to_vec());
            }

            let data_size = encoded.iter().map(|b| b.len()).sum::<usize>();

            span.record("data_size", data_size);
            span.record("blocks_count", encoded.len());
            span.record("transactions_count", transactions_count);
            span.record("validators_count", validators_count);
            span.record("blobs_count", blobs_count);

            let data = SendData {
                cursor: block.cursor,
                end_cursor: block.end_cursor,
                data: encoded,
            };

            if cb(data).await == ScannerAction::Stop {
                return Ok(());
            }
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "stream_send_block",
        skip_all,
        err(Debug),
        fields(cursor = %cursor, data_size, blocks_count, transactions_count, validators_count, blobs_count)
    )]
    async fn scan_single<S>(&mut self, cursor: &Cursor, cb: S) -> Result<(), ScannerError>
    where
        S: FnOnce(Vec<Vec<u8>>) + Send,
    {
        debug!(cursor = %cursor, "scanning single block");
        let block_content = self
            .store
            .get_block(cursor)
            .await
            .change_context(ScannerError)
            .attach_printable("failed to get block")?;

        let block = rkyv::access::<
            rkyv::Archived<fragment::Slot<store::block::Block>>,
            rkyv::rancor::Error,
        >(&block_content)
        .change_context(ScannerError)
        .attach_printable("failed to access block data")?;

        let fragment::ArchivedSlot::Proposed(block) = block else {
            return Ok(());
        };

        let filtered = self
            .filters
            .iter()
            .map(|f| f.filter_data(&block.index))
            .collect::<Result<Vec<_>, _>>()?;

        if filtered.has_no_data() {
            return Ok(());
        }

        let mut encoded = Vec::with_capacity(filtered.len());

        let mut transactions_count = 0;
        let mut validators_count = 0;
        let mut blobs_count = 0;

        for mut data in filtered {
            let mut result = beaconchain::Block::default();

            let header: beaconchain::BlockHeader = (&block.header).into();
            result.header = Some(header);

            let block_transactions = &block.transactions;
            let block_validators = &block.validators;
            let block_blobs = &block.blobs;

            for data_ref in data
                .transactions_by_blob
                .take_matches(block_transactions.len())
            {
                for blob in block_blobs.iter() {
                    if blob.transaction_index == data_ref.index {
                        data.transactions.add_data_ref(data_ref);
                        break;
                    }
                }
            }

            for data_ref in data.blobs_by_transaction.take_matches(block_blobs.len()) {
                for transaction in block_transactions.iter() {
                    if transaction.transaction_index == data_ref.index {
                        data.blobs.add_data_ref(data_ref);
                        break;
                    }
                }
            }

            for data_ref in data.transactions.take_matches(block_transactions.len()) {
                let mut transaction: beaconchain::Transaction =
                    (&block_transactions[data_ref.index as usize]).into();
                transaction.filter_ids = data_ref.filter_ids.into_iter().collect();
                result.transactions.push(transaction);
            }

            for data_ref in data.validators.take_matches(block_validators.len()) {
                let mut validator: beaconchain::Validator =
                    (&block_validators[data_ref.index as usize]).into();
                validator.filter_ids = data_ref.filter_ids.into_iter().collect();
                result.validators.push(validator);
            }

            for data_ref in data.blobs.take_matches(block_blobs.len()) {
                let mut blob: beaconchain::Blob = (&block_blobs[data_ref.index as usize]).into();
                blob.filter_ids = data_ref.filter_ids.into_iter().collect();
                result.blobs.push(blob);
            }

            transactions_count += result.transactions.len();
            validators_count += result.validators.len();
            blobs_count += result.blobs.len();

            encoded.push(result.encode_to_vec());
        }

        let data_size = encoded.iter().map(|b| b.len()).sum::<usize>();

        let current_span = tracing::Span::current();
        current_span.record("data_size", data_size);
        current_span.record("blocks_count", encoded.len());
        current_span.record("transactions_count", transactions_count);
        current_span.record("validators_count", validators_count);
        current_span.record("blobs_count", blobs_count);

        cb(encoded);

        Ok(())
    }
}
