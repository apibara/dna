use std::collections::{BTreeMap, BTreeSet};

use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};
use rkyv::Deserialize;

use crate::segment::store;

use super::error::SegmentFilterError;

/// Holds the data shared by one or more blocks.
#[derive(Default, Debug)]
pub struct DataBag {
    validators: BTreeMap<u32, beaconchain::Validator>,
    transactions: BTreeMap<u32, beaconchain::Transaction>,
    blobs: BTreeMap<u32, beaconchain::Blob>,

    deferred_transactions: BTreeSet<u32>,
    deferred_transaction_blobs: BTreeSet<u32>,
}

impl DataBag {
    pub fn add_validator(&mut self, index: u32, validator: store::Validator) {
        if self.validators.contains_key(&index) {
            return;
        }

        let validator = beaconchain::Validator::from(validator);
        self.validators.insert(index, validator);
    }

    pub fn validator(&mut self, index: &u32) -> Option<beaconchain::Validator> {
        self.validators.get(index).cloned()
    }

    pub fn add_blob(
        &mut self,
        index: u32,
        blob: &store::ArchivedBlob,
    ) -> Result<(), SegmentFilterError> {
        if self.blobs.contains_key(&index) {
            return Ok(());
        }

        let blob: store::Blob = blob
            .deserialize(&mut rkyv::Infallible)
            .change_context(SegmentFilterError)
            .attach_printable("failed to deserialize archived blob")?;
        let blob = beaconchain::Blob::from(blob);

        self.blobs.insert(index, blob);

        Ok(())
    }

    pub fn blob(&self, index: &u32) -> Option<beaconchain::Blob> {
        self.blobs.get(index).cloned()
    }

    pub fn defer_transaction_blob(&mut self, transaction_index: u32) {
        self.deferred_transaction_blobs.insert(transaction_index);
    }

    pub fn has_deferred_transaction_blob(&self, index: u32) -> bool {
        self.deferred_transaction_blobs.contains(&index)
    }

    pub fn has_deferred_transaction_blobs(&self) -> bool {
        !self.deferred_transaction_blobs.is_empty()
    }

    pub fn add_transaction(
        &mut self,
        index: u32,
        transaction: &store::ArchivedTransaction,
    ) -> Result<(), SegmentFilterError> {
        if self.transactions.contains_key(&index) {
            return Ok(());
        }

        let transaction: store::Transaction = transaction
            .deserialize(&mut rkyv::Infallible)
            .change_context(SegmentFilterError)
            .attach_printable("failed to deserialize archived transaction")?;
        let transaction = beaconchain::Transaction::from(transaction);

        self.transactions.insert(index, transaction);

        Ok(())
    }

    pub fn defer_transaction(&mut self, transaction_index: u32) {
        self.deferred_transactions.insert(transaction_index);
    }

    pub fn has_deferred_transactions(&self) -> bool {
        !self.deferred_transactions.is_empty()
    }

    pub fn has_deferred_transaction(&self, index: u32) -> bool {
        self.deferred_transactions.contains(&index)
    }

    pub fn transaction(&self, index: &u32) -> Option<beaconchain::Transaction> {
        self.transactions.get(index).cloned()
    }
}
