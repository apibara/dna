use apibara_dna_common::{
    data_stream::{FilterMatch, FilterMatchSet, ScannerError},
    store::index::ArchivedIndexGroup,
};
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use rkyv::de::Pool;
use roaring::RoaringBitmap;

use crate::{
    provider::models::ValidatorStatus,
    store::{fragment, index},
};

#[derive(Debug)]
pub struct Filter {
    always_include_header: bool,
    transactions: Vec<TransactionFilter>,
    validators: Vec<ValidatorFilter>,
    blobs: Vec<BlobFilter>,
}

#[derive(Debug)]
enum ToAddress {
    Any,
    Exact(fragment::Address),
    Create,
}

#[derive(Debug)]
struct TransactionFilter {
    pub id: u32,
    pub from: Option<fragment::Address>,
    pub to: ToAddress,
    pub include_blob: bool,
}

#[derive(Debug)]
struct ValidatorFilter {
    pub id: u32,
    pub validator_index: Option<u32>,
    pub status: Option<ValidatorStatus>,
}

#[derive(Debug)]
struct BlobFilter {
    pub id: u32,
    pub include_transaction: bool,
}

#[derive(Debug)]
pub struct FilteredData {
    pub header: bool,
    pub transactions: FilterMatchSet,
    pub transactions_by_blob: FilterMatchSet,
    pub blobs: FilterMatchSet,
    pub blobs_by_transaction: FilterMatchSet,
    pub validators: FilterMatchSet,
}

impl FilteredData {
    pub fn is_empty(&self) -> bool {
        !self.header
            && self.transactions.is_empty()
            && self.transactions_by_blob.is_empty()
            && self.blobs.is_empty()
            && self.blobs_by_transaction.is_empty()
            && self.validators.is_empty()
    }
}

pub trait FilteredDataExt {
    fn has_no_data(&self) -> bool;
}

impl FilteredDataExt for Vec<FilteredData> {
    fn has_no_data(&self) -> bool {
        self.iter().all(FilteredData::is_empty)
    }
}

impl Filter {
    /// Returns `true` if the filter will never match any data.
    pub fn is_null(&self) -> bool {
        !self.always_include_header
            && self.transactions.is_empty()
            && self.validators.is_empty()
            && self.blobs.is_empty()
    }

    pub fn filter_data(&self, index: &ArchivedIndexGroup) -> Result<FilteredData, ScannerError> {
        let mut deserializer = Pool::default();

        let mut local = FilterMatch::default();

        let mut transactions = FilterMatchSet::default();
        let mut transactions_by_blob = FilterMatchSet::default();

        let mut blobs = FilterMatchSet::default();
        let mut blobs_by_transaction = FilterMatchSet::default();

        let mut validators = FilterMatchSet::default();

        if !self.transactions.is_empty() {
            let transaction_by_from_address = index
                .access_archived_index::<index::IndexTransactionByFromAddress>()
                .change_context(ScannerError)?
                .ok_or(ScannerError)
                .attach_printable("missing transaction by from address index")?
                .deserialize(&mut deserializer)
                .change_context(ScannerError)?;

            let transaction_by_to_address = index
                .access_archived_index::<index::IndexTransactionByToAddress>()
                .change_context(ScannerError)?
                .ok_or(ScannerError)
                .attach_printable("missing transaction by to address index")?
                .deserialize(&mut deserializer)
                .change_context(ScannerError)?;

            let transaction_by_create = index
                .access_archived_index::<index::IndexTransactionByCreate>()
                .change_context(ScannerError)?
                .ok_or(ScannerError)
                .attach_printable("missing transaction by create index")?
                .deserialize(&mut deserializer)
                .change_context(ScannerError)?;

            for tx_filter in &self.transactions {
                local.reset();

                if let Some(from_address) = tx_filter.from {
                    if let Some(bitmap) = transaction_by_from_address
                        .get_bitmap(&from_address)
                        .change_context(ScannerError)?
                    {
                        local.intersect(&bitmap);
                    } else {
                        local.clear();
                    }
                }

                match tx_filter.to {
                    ToAddress::Any => {}
                    ToAddress::Exact(to_address) => {
                        if let Some(bitmap) = transaction_by_to_address
                            .get_bitmap(&to_address)
                            .change_context(ScannerError)?
                        {
                            local.intersect(&bitmap);
                        } else {
                            local.clear();
                        }
                    }
                    ToAddress::Create => {
                        if let Some(bitmap) = transaction_by_create
                            .get_bitmap(&())
                            .change_context(ScannerError)?
                        {
                            local.intersect(&bitmap);
                        } else {
                            local.clear();
                        }
                    }
                }

                transactions.add_match(tx_filter.id, &local);

                if tx_filter.include_blob {
                    blobs_by_transaction.add_match(tx_filter.id, &local);
                }
            }
        }

        for blob_filter in &self.blobs {
            local.reset();
            blobs.add_match(blob_filter.id, &local);

            if blob_filter.include_transaction {
                transactions_by_blob.add_match(blob_filter.id, &local);
            }
        }

        if !self.validators.is_empty() {
            let validator_by_status = index
                .access_archived_index::<index::IndexValidatorByStatus>()
                .change_context(ScannerError)?
                .ok_or(ScannerError)
                .attach_printable("missing validator by status index")?
                .deserialize(&mut deserializer)
                .change_context(ScannerError)?;

            for validator_filter in &self.validators {
                local.reset();

                if let Some(index) = validator_filter.validator_index {
                    let mut bitmap = RoaringBitmap::new();
                    bitmap.insert(index);
                    local.intersect(&bitmap);
                }

                if let Some(status) = validator_filter.status {
                    if let Some(bitmap) = validator_by_status
                        .get_bitmap(&status)
                        .change_context(ScannerError)?
                    {
                        local.intersect(&bitmap);
                    } else {
                        local.clear();
                    }
                }

                validators.add_match(validator_filter.id, &local);
            }
        }

        Ok(FilteredData {
            header: self.always_include_header,
            transactions,
            transactions_by_blob,
            blobs,
            blobs_by_transaction,
            validators,
        })
    }
}

impl From<beaconchain::TransactionFilter> for TransactionFilter {
    fn from(v: beaconchain::TransactionFilter) -> Self {
        let from = v.from.map(fragment::Address::from);
        let include_blob = v.include_blob.unwrap_or(false);

        let to = match (v.to, v.create.unwrap_or(false)) {
            (None, false) => ToAddress::Any,
            (None, true) => ToAddress::Create,
            (Some(to), _) => ToAddress::Exact(fragment::Address::from(to)),
        };

        Self {
            id: v.id,
            from,
            to,
            include_blob,
        }
    }
}

impl From<beaconchain::ValidatorFilter> for ValidatorFilter {
    fn from(v: beaconchain::ValidatorFilter) -> Self {
        let validator_index = v.validator_index;
        let status = v
            .status
            .map(|s| s.try_into())
            .transpose()
            .unwrap_or_default();

        Self {
            id: v.id,
            validator_index,
            status,
        }
    }
}

impl From<beaconchain::BlobFilter> for BlobFilter {
    fn from(v: beaconchain::BlobFilter) -> Self {
        let include_transaction = v.include_transaction.unwrap_or(false);

        Self {
            id: v.id,
            include_transaction,
        }
    }
}

impl From<beaconchain::Filter> for Filter {
    fn from(v: beaconchain::Filter) -> Self {
        let always_include_header = v.header.and_then(|h| h.always).unwrap_or(false);

        let transactions = v
            .transactions
            .into_iter()
            .map(TransactionFilter::from)
            .collect();

        let validators = v
            .validators
            .into_iter()
            .map(ValidatorFilter::from)
            .collect();

        let blobs = v.blobs.into_iter().map(BlobFilter::from).collect();

        Self {
            always_include_header,
            transactions,
            validators,
            blobs,
        }
    }
}
