mod filter;
mod proto_conversion;

use std::ops::RangeInclusive;

use apibara_dna_common::{
    block_store::BlockStoreReader,
    data_stream::{Scanner, ScannerError, ScannerFactory},
    Cursor,
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use filter::{Filter, FilteredDataExt};
use prost::Message;
use roaring::RoaringBitmap;
use tracing::info;

use crate::store::{self, fragment};

pub type BeaconchainBlockStoreReader = BlockStoreReader<fragment::Slot<store::block::Block>>;

const MAX_FILTERS_LEN: usize = 5;

pub struct BeaconChainScannerFactory {
    store: BeaconchainBlockStoreReader,
}

pub struct BeaconChainScanner {
    filters: Vec<Filter>,
    store: BeaconchainBlockStoreReader,
}

impl BeaconChainScannerFactory {
    pub fn new(store: BeaconchainBlockStoreReader) -> Self {
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
    async fn fill_block_bitmap(
        &mut self,
        _bitmap: &mut RoaringBitmap,
        _block_range: RangeInclusive<u32>,
    ) -> Result<(), ScannerError> {
        Ok(())
    }

    #[tracing::instrument(name = "stream_send_block", skip_all, err(Debug), fields(cursor = %cursor, data_size))]
    async fn scan_single<S>(&mut self, cursor: &Cursor, cb: S) -> Result<(), ScannerError>
    where
        S: FnOnce(Vec<Vec<u8>>) + Send,
    {
        info!(cursor = %cursor, "scanning single block");
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

            println!("txs {:?}", result.transactions.len());
            println!("blobs {:?}", result.blobs.len());
            println!("valid {:?}", result.validators.len());

            encoded.push(result.encode_to_vec());
        }

        let data_size = encoded.iter().map(|b| b.len()).sum::<usize>();

        let current_span = tracing::Span::current();
        current_span.record("data_size", data_size);

        cb(encoded);

        Ok(())
    }
}
