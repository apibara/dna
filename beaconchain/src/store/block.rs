use apibara_dna_common::store::{
    bitmap::BitmapMapBuilder,
    fragment::{Block, Fragment},
    index::IndexGroup,
};
use error_stack::{Result, ResultExt};

use super::{
    error::StoreError,
    fragment::{Blob, BlockHeader, Slot, Transaction, Validator},
    index::{
        IndexTransactionByCreate, IndexTransactionByFromAddress, IndexTransactionByToAddress,
        IndexValidatorByStatus,
    },
};

pub struct BlockBuilder {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}

impl Fragment for Slot<BlockHeader> {
    fn tag() -> u8 {
        1
    }

    fn name() -> &'static str {
        "header"
    }
}

impl Fragment for Slot<Vec<Transaction>> {
    fn tag() -> u8 {
        2
    }

    fn name() -> &'static str {
        "transaction"
    }
}

impl Fragment for Slot<Vec<Validator>> {
    fn tag() -> u8 {
        3
    }

    fn name() -> &'static str {
        "validator"
    }
}

impl Fragment for Slot<Vec<Blob>> {
    fn tag() -> u8 {
        4
    }

    fn name() -> &'static str {
        "blob"
    }
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

        let transaction_by_from_address = transaction_by_from_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_to_address = transaction_by_to_address
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;
        let transaction_by_create = transaction_by_create
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexTransactionByFromAddress>(&transaction_by_from_address)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByToAddress>(&transaction_by_to_address)
            .change_context(StoreError::Indexing)?;
        index
            .add_index::<IndexTransactionByCreate>(&transaction_by_create)
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

        let validator_by_status = validator_by_status
            .into_bitmap_map()
            .change_context(StoreError::Indexing)?;

        index
            .add_index::<IndexValidatorByStatus>(&validator_by_status)
            .change_context(StoreError::Indexing)?;

        Ok(())
    }
}
