mod filter;
mod proto_conversion;

use std::ops::RangeInclusive;

use apibara_dna_common::{
    data_stream::{FragmentAccess, Scanner, ScannerError, ScannerFactory},
    store::{group::ArchivedSegmentGroup, index::IndexGroup},
    Cursor,
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use filter::{Filter, FilteredDataExt};
use prost::Message;
use roaring::RoaringBitmap;
use tracing::{debug, warn};

use crate::store::fragment::{self, Slot};

const MAX_FILTERS_LEN: usize = 5;

#[derive(Default)]
pub struct BeaconChainScannerFactory;

pub struct BeaconChainScanner {
    filters: Vec<Filter>,
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

        Ok(BeaconChainScanner { filters })
    }
}

impl Scanner for BeaconChainScanner {
    async fn fill_block_bitmap(
        &mut self,
        group: &ArchivedSegmentGroup,
        bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) -> Result<(), ScannerError> {
        debug!("filling block bitmap");

        if group.index.indices.is_empty() {
            warn!("group is empty. this should not happen.");
            return Ok(());
        }

        let filtered = self
            .filters
            .iter()
            .map(|f| f.filter_data(&group.index, true))
            .collect::<Result<Vec<_>, _>>()?;

        for filter in filtered {
            if filter.header {
                bitmap.insert_range(block_range);
                break;
            }

            for match_ in filter.transactions.iter() {
                bitmap.insert(match_.index);
            }

            for match_ in filter.validators.iter() {
                bitmap.insert(match_.index);
            }

            for match_ in filter.blobs.iter() {
                bitmap.insert(match_.index);
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
    async fn scan_single<S>(
        &mut self,
        cursor: &Cursor,
        fragment_access: &FragmentAccess,
        cb: S,
    ) -> Result<(), ScannerError>
    where
        S: FnOnce(Vec<Vec<u8>>) + Send,
    {
        debug!(cursor = %cursor, "scanning single block");

        let header = fragment_access
            .get_fragment::<Slot<fragment::BlockHeader>>()
            .await
            .change_context(ScannerError)?;
        let header = header.access().change_context(ScannerError)?;

        let fragment::ArchivedSlot::Proposed(header) = header else {
            return Ok(());
        };

        let index = fragment_access
            .get_fragment::<IndexGroup>()
            .await
            .change_context(ScannerError)?;
        let index = index.access().change_context(ScannerError)?;

        let filtered = self
            .filters
            .iter()
            .map(|f| f.filter_data(index, false))
            .collect::<Result<Vec<_>, _>>()?;

        if filtered.has_no_data() {
            return Ok(());
        }

        let mut encoded = Vec::with_capacity(filtered.len());

        let mut transactions_count = 0;
        let mut validators_count = 0;
        let mut blobs_count = 0;

        let header: beaconchain::BlockHeader = header.into();

        for data in filtered {
            let mut result = beaconchain::Block {
                header: Some(header.clone()),
                ..Default::default()
            };

            if !data.transactions.is_empty() {
                let transactions = fragment_access
                    .get_fragment::<Slot<Vec<fragment::Transaction>>>()
                    .await
                    .change_context(ScannerError)?;
                let transactions = transactions
                    .access()
                    .change_context(ScannerError)?
                    .as_proposed()
                    .ok_or(ScannerError)
                    .attach_printable("missing transactions")?;

                for match_ in data.transactions.iter() {
                    let mut transaction: beaconchain::Transaction =
                        (&transactions[match_.index as usize]).into();
                    transaction.filter_ids = match_.filter_ids;
                    result.transactions.push(transaction);
                }
            }

            if !data.validators.is_empty() {
                let validators = fragment_access
                    .get_fragment::<Slot<Vec<fragment::Validator>>>()
                    .await
                    .change_context(ScannerError)?;
                let validators = validators
                    .access()
                    .change_context(ScannerError)?
                    .as_proposed()
                    .ok_or(ScannerError)
                    .attach_printable("missing validators")?;

                for match_ in data.validators.iter() {
                    let mut validator: beaconchain::Validator =
                        (&validators[match_.index as usize]).into();
                    validator.filter_ids = match_.filter_ids;
                    result.validators.push(validator);
                }
            }

            if !data.blobs.is_empty() {
                let blobs = fragment_access
                    .get_fragment::<Slot<Vec<fragment::Blob>>>()
                    .await
                    .change_context(ScannerError)?;
                let blobs = blobs
                    .access()
                    .change_context(ScannerError)?
                    .as_proposed()
                    .ok_or(ScannerError)
                    .attach_printable("missing blobs")?;

                for match_ in data.blobs.iter() {
                    let mut blob: beaconchain::Blob = (&blobs[match_.index as usize]).into();
                    blob.filter_ids = match_.filter_ids;
                    result.blobs.push(blob);
                }
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