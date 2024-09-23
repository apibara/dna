use apibara_dna_common::store::{
    bitmap::{BitmapMapBuilder, RoaringBitmapExt},
    fragment::Block,
    index::IndexGroup,
    StoreError,
};
use error_stack::{Result, ResultExt};
use roaring::RoaringBitmap;

use super::{
    fragment::{Blob, BlockHeader, Slot, Transaction, Validator},
    index::{
        IndexBlobByTransactionIndex, IndexTransactionByBlobIndex, IndexTransactionByCreate,
        IndexTransactionByFromAddress, IndexTransactionByToAddress, IndexValidatorByStatus,
    },
};

pub struct BlockBuilder {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}

impl BlockBuilder {
    pub fn new(header: BlockHeader) -> Self {
        Self {
            header,
            transactions: Vec::new(),
            validators: Vec::new(),
            blobs: Vec::new(),
        }
    }

    pub fn add_transactions(&mut self, transactions: Vec<Transaction>) {
        self.transactions.extend(transactions);
    }

    pub fn add_validators(&mut self, validators: Vec<Validator>) {
        self.validators.extend(validators);
    }

    pub fn add_blobs(&mut self, blobs: Vec<Blob>) {
        self.blobs.extend(blobs);
    }

    pub fn build(self) -> Result<Block, StoreError> {
        let mut index = IndexGroup::default();

        self.index_transactions(&mut index)?;
        self.index_validators(&mut index)?;
        self.index_blobs(&mut index)?;

        let header_fragment = Slot::Proposed(self.header);
        let transactions_fragment = Slot::Proposed(self.transactions);
        let validators_fragment = Slot::Proposed(self.validators);
        let blobs_fragment = Slot::Proposed(self.blobs);

        let mut block = Block::default();
        block
            .add_fragment(index)
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(header_fragment)
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(transactions_fragment)
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(validators_fragment)
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment(blobs_fragment)
            .change_context(StoreError::Fragment)?;

        Ok(block)
    }

    pub fn missed_block(slot: u64) -> Result<Block, StoreError> {
        let index = IndexGroup::default();

        let mut block = Block::default();

        block
            .add_fragment(index)
            .change_context(StoreError::Fragment)?;

        block
            .add_fragment::<Slot<BlockHeader>>(Slot::Missed { slot })
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment::<Slot<Vec<Transaction>>>(Slot::Missed { slot })
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment::<Slot<Vec<Validator>>>(Slot::Missed { slot })
            .change_context(StoreError::Fragment)?;
        block
            .add_fragment::<Slot<Vec<Blob>>>(Slot::Missed { slot })
            .change_context(StoreError::Fragment)?;

        Ok(block)
    }

    fn index_transactions(&self, index: &mut IndexGroup) -> Result<(), StoreError> {
        let mut transaction_by_from_address = BitmapMapBuilder::default();
        let mut transaction_by_to_address = BitmapMapBuilder::default();
        let mut transaction_by_create = BitmapMapBuilder::default();
        let mut transaction_by_blob_index = BitmapMapBuilder::default();

        for tx in &self.transactions {
            transaction_by_from_address
                .entry(tx.from)
                .insert(tx.transaction_index);

            if let Some(to) = tx.to {
                transaction_by_to_address
                    .entry(to)
                    .insert(tx.transaction_index);
            } else {
                transaction_by_create.entry(()).insert(tx.transaction_index);
            }
        }

        for blob in &self.blobs {
            transaction_by_blob_index
                .entry(blob.blob_index)
                .insert(blob.transaction_index);
        }

        let transaction_by_from_address = transaction_by_from_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_to_address = transaction_by_to_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_create = transaction_by_create
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_blob_index = transaction_by_blob_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        let transactions_length = self.transactions.len();
        let transaction_range = RoaringBitmap::from_iter(0..transactions_length as u32)
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexTransactionByFromAddress>(
                transaction_range.clone(),
                transaction_by_from_address,
            )
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByToAddress>(
                transaction_range.clone(),
                transaction_by_to_address,
            )
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByCreate>(transaction_range.clone(), transaction_by_create)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByBlobIndex>(transaction_range, transaction_by_blob_index)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }

    fn index_validators(&self, index: &mut IndexGroup) -> Result<(), StoreError> {
        let mut validator_by_status = BitmapMapBuilder::default();

        for validator in &self.validators {
            validator_by_status
                .entry(validator.status)
                .insert(validator.validator_index);
        }

        let validators_length = self.validators.len();
        let validator_range = RoaringBitmap::from_iter(0..validators_length as u32)
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        let validator_by_status = validator_by_status
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexValidatorByStatus>(validator_range, validator_by_status)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }

    fn index_blobs(&self, index: &mut IndexGroup) -> Result<(), StoreError> {
        let mut blob_by_transaction_index = BitmapMapBuilder::default();

        for blob in &self.blobs {
            blob_by_transaction_index
                .entry(blob.transaction_index)
                .insert(blob.blob_index);
        }

        let blobs_length = self.blobs.len();
        let blob_range = RoaringBitmap::from_iter(0..blobs_length as u32)
            .into_bitmap()
            .change_context(StoreError::Indexing)?;

        let blob_by_transaction_index = blob_by_transaction_index
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexBlobByTransactionIndex>(blob_range, blob_by_transaction_index)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }
}
