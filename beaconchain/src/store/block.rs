use apibara_dna_common::{
    store::{
        bitmap::BitmapMapBuilder,
        index::{IndexGroup, TaggedIndex},
    },
    Cursor,
};
use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Serialize};

use crate::provider::models;

use super::{
    error::StoreError,
    fragment::{self, Blob, BlockHeader, Transaction, Validator},
};

/// A block containing all fragments for a single block.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Block {
    pub index: IndexGroup,
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub validators: Vec<Validator>,
    pub blobs: Vec<Blob>,
}

/// FromAddress -> TransactionIndex
pub struct IndexTransactionByFromAddress;

/// ToAddress -> TransactionIndex
pub struct IndexTransactionByToAddress;

/// CreateTransaction -> TransactionIndex
pub struct IndexTransactionByCreate;

/// ValidatorStatus -> ValidatorIndex
pub struct IndexValidatorByStatus;

impl Block {
    pub fn cursor(&self) -> Cursor {
        let slot = self.header.slot;
        let hash = self.header.state_root.0.to_vec();
        Cursor::new(slot, hash)
    }
}

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

        Ok(Block {
            index,
            header: self.header,
            transactions: self.transactions,
            validators: self.validators,
            blobs: self.blobs,
        })
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

impl fragment::Slot<Block> {
    pub fn cursor(&self) -> Cursor {
        match self {
            fragment::Slot::Missed { slot } => Cursor::new(*slot, Vec::new()),
            fragment::Slot::Proposed(block) => block.cursor(),
        }
    }
}

impl TaggedIndex for IndexTransactionByFromAddress {
    type Key = fragment::Address;

    fn tag() -> u8 {
        1
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

    fn name() -> &'static str {
        "transaction_by_to_address"
    }
}

impl TaggedIndex for IndexTransactionByCreate {
    type Key = ();

    fn tag() -> u8 {
        3
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

    fn name() -> &'static str {
        "validator_by_status"
    }
}
