use apibara_dna_common::{
    store::bitmap::{Bitmap, BitmapMap, BitmapMapBuilder, RoaringBitmapExt},
    Cursor,
};
use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Serialize};
use roaring::RoaringBitmap;

use crate::provider::models;

use super::{
    error::StoreError,
    fragment::{self, Blob, BlockHeader, Transaction, Validator},
};

/// A block containing all fragments for a single block.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}

/// A block with data and indices.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct IndexedBlock {
    pub transaction: TransactionIndex,
    pub validator: ValidatorIndex,
    pub blob: BlobIndex,
    pub block: Block,
}

/// Index transactions.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct TransactionIndex {
    /// Transactions by from address.
    pub by_from_address: BitmapMap<fragment::Address>,
    /// Transactions by to address.
    pub by_to_address: BitmapMap<fragment::Address>,
    /// Transactions where to address is empty.
    pub by_create: Bitmap,
}

/// Index validators.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct ValidatorIndex {
    /// Validators by status.
    pub by_status: BitmapMap<models::ValidatorStatus>,
}

/// Index blobs.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct BlobIndex {
    // Blob has no indices.
}

impl Block {
    pub fn cursor(&self) -> Cursor {
        let slot = self.header.slot;
        let hash = self.header.state_root.0.to_vec();
        Cursor::new(slot, hash)
    }

    pub fn into_indexed_block(self) -> Result<IndexedBlock, StoreError> {
        let transaction = index_transactions(&self.transactions)?;
        let validator = index_validators(&self.validators)?;
        let blob = index_blobs(&self.blobs)?;

        Ok(IndexedBlock {
            transaction,
            validator,
            blob,
            block: self,
        })
    }
}

fn index_transactions(transactions: &[Transaction]) -> Result<TransactionIndex, StoreError> {
    let mut by_from_address = BitmapMapBuilder::default();
    let mut by_to_address = BitmapMapBuilder::default();
    let mut by_create = RoaringBitmap::default();

    for tx in transactions {
        by_from_address.entry(tx.from).insert(tx.transaction_index);

        if let Some(to) = tx.to {
            by_to_address.entry(to).insert(tx.transaction_index);
        } else {
            by_create.insert(tx.transaction_index);
        }
    }

    Ok(TransactionIndex {
        by_from_address: by_from_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?,
        by_to_address: by_to_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?,
        by_create: by_create
            .into_bitmap()
            .change_context(StoreError::Indexing)?,
    })
}

fn index_validators(validators: &[Validator]) -> Result<ValidatorIndex, StoreError> {
    let mut by_status = BitmapMapBuilder::default();

    for validator in validators {
        by_status
            .entry(validator.status)
            .insert(validator.validator_index);
    }

    Ok(ValidatorIndex {
        by_status: by_status
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?,
    })
}

fn index_blobs(_blobs: &[Blob]) -> Result<BlobIndex, StoreError> {
    Ok(BlobIndex {})
}
