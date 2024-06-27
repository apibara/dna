use apibara_dna_protocol::beaconchain;
use roaring::RoaringBitmap;

use crate::segment::store;

use super::{blob::BlobFilter, transaction::TransactionFilter, validator::ValidatorFilter};

pub struct Filter {
    header: HeaderFilter,
    validators: Vec<ValidatorFilter>,
    transactions: Vec<TransactionFilter>,
    blobs: Vec<BlobFilter>,
}

#[derive(Default)]
pub struct HeaderFilter {
    always: bool,
}

#[derive(Default)]
pub struct AdditionalData {
    pub include_transaction: bool,
    pub include_blob: bool,
}

impl Filter {
    pub fn needs_linear_scan(&self) -> bool {
        self.has_required_header() || self.has_validators() || self.has_blobs()
    }

    pub fn has_required_header(&self) -> bool {
        self.header.always
    }

    pub fn has_validators(&self) -> bool {
        !self.validators.is_empty()
    }

    pub fn has_transactions(&self) -> bool {
        !self.transactions.is_empty()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &TransactionFilter> {
        self.transactions.iter()
    }

    pub fn has_blobs(&self) -> bool {
        !self.blobs.is_empty()
    }

    pub fn fill_validator_bitmap(
        &self,
        validators_count: usize,
        index: &store::ValidatorsIndex,
        bitmap: &mut RoaringBitmap,
    ) -> Result<(), std::io::Error> {
        for filter in &self.validators {
            filter.fill_validator_bitmap(validators_count, index, bitmap)?;
        }

        Ok(())
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
                additional_data.include_blob |= filter.include_blob;
            }
        }

        if any_match {
            Some(additional_data)
        } else {
            None
        }
    }

    pub fn match_blob(&self, _blob: &store::ArchivedBlob) -> Option<AdditionalData> {
        if self.blobs.is_empty() {
            return None;
        }

        let mut additional_data = AdditionalData::default();

        for filter in &self.blobs {
            additional_data.include_transaction |= filter.include_transaction;
        }

        Some(additional_data)
    }
}

impl From<beaconchain::Filter> for Filter {
    fn from(filter: beaconchain::Filter) -> Self {
        let header = filter.header.map(HeaderFilter::from).unwrap_or_default();
        let validators = filter
            .validators
            .into_iter()
            .map(ValidatorFilter::from)
            .collect();
        let transactions = filter
            .transactions
            .into_iter()
            .map(TransactionFilter::from)
            .collect();
        let blobs = filter.blobs.into_iter().map(BlobFilter::from).collect();

        Self {
            header,
            validators,
            transactions,
            blobs,
        }
    }
}

impl From<beaconchain::HeaderFilter> for HeaderFilter {
    fn from(value: beaconchain::HeaderFilter) -> Self {
        Self {
            always: value.always.unwrap_or(false),
        }
    }
}
