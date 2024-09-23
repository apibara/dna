use apibara_dna_common::store::index::TaggedIndex;

use crate::provider::models;

use super::fragment;

/// FromAddress -> TransactionIndex
///
/// Used by: TransactionFilter.from
pub struct IndexTransactionByFromAddress;

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

/// ToAddress -> TransactionIndex
///
/// Used by: TransactionFilter.to
pub struct IndexTransactionByToAddress;

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

/// CreateTransaction -> TransactionIndex
///
/// Used by: TransactionFilter.create
pub struct IndexTransactionByCreate;

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

/// BlobIndex -> TransactionIndex
///
/// Used by: TransactionFilter.include_blob
pub struct IndexTransactionByBlobIndex;

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

/// ValidatorStatus -> ValidatorIndex
///
/// Used by: ValidatorFilter.status
pub struct IndexValidatorByStatus;

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

/// TransactionIndex -> BlobIndex
///
/// Used by: BlobFilter.include_transaction
pub struct IndexBlobByTransactionIndex;

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
