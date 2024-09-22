use apibara_dna_common::store::index::TaggedIndex;

use crate::provider::models;

use super::fragment;

/// FromAddress -> TransactionIndex
///
/// Used by: TransactionFilter.from
pub struct IndexTransactionByFromAddress;

/// ToAddress -> TransactionIndex
///
/// Used by: TransactionFilter.to
pub struct IndexTransactionByToAddress;

/// CreateTransaction -> TransactionIndex
///
/// Used by: TransactionFilter.create
pub struct IndexTransactionByCreate;

/// BlobIndex -> TransactionIndex
///
/// Used by: TransactionFilter.include_blob
pub struct IndexTransactionByBlobIndex;

/// ValidatorStatus -> ValidatorIndex
///
/// Used by: ValidatorFilter.status
pub struct IndexValidatorByStatus;

/// TransactionIndex -> BlobIndex
///
/// Used by: BlobFilter.include_transaction
pub struct IndexBlobByTransactionIndex;

impl TaggedIndex for IndexTransactionByFromAddress {
    type Key = fragment::Address;

    fn tag() -> u8 {
        1
    }

    fn key_size() -> usize {
        20
    }

    fn name() -> &'static str {
        "transaction_by_from_address"
    }
}

impl TaggedIndex for IndexTransactionByToAddress {
    type Key = fragment::Address;

    fn tag() -> u8 {
        2
    }

    fn key_size() -> usize {
        20
    }

    fn name() -> &'static str {
        "transaction_by_to_address"
    }
}

impl TaggedIndex for IndexTransactionByCreate {
    type Key = ();

    fn tag() -> u8 {
        3
    }

    fn key_size() -> usize {
        0
    }

    fn name() -> &'static str {
        "transaction_by_create"
    }
}

impl TaggedIndex for IndexTransactionByBlobIndex {
    type Key = u32;

    fn tag() -> u8 {
        4
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "transaction_by_blob_index"
    }
}

impl TaggedIndex for IndexValidatorByStatus {
    type Key = models::ValidatorStatus;

    fn tag() -> u8 {
        5
    }

    fn key_size() -> usize {
        1
    }

    fn name() -> &'static str {
        "validator_by_status"
    }
}

impl TaggedIndex for IndexBlobByTransactionIndex {
    type Key = u32;

    fn tag() -> u8 {
        6
    }

    fn key_size() -> usize {
        4
    }

    fn name() -> &'static str {
        "blob_by_transaction_index"
    }
}
