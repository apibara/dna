use apibara_dna_common::store::index::TaggedIndex;

use crate::provider::models;

use super::fragment;

/// FromAddress -> TransactionIndex
pub struct IndexTransactionByFromAddress;

/// ToAddress -> TransactionIndex
pub struct IndexTransactionByToAddress;

/// CreateTransaction -> TransactionIndex
pub struct IndexTransactionByCreate;

/// ValidatorStatus -> ValidatorIndex
pub struct IndexValidatorByStatus;

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

impl TaggedIndex for IndexValidatorByStatus {
    type Key = models::ValidatorStatus;

    fn tag() -> u8 {
        4
    }

    fn key_size() -> usize {
        1
    }

    fn name() -> &'static str {
        "validator_by_status"
    }
}
