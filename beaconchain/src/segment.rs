use apibara_dna_common::{
    store::{
        index::IndexGroup,
        segment::{Fragment, IndexSegment, Segment, SegmentError, SerializedSegment},
    },
    Cursor,
};
use error_stack::{Result, ResultExt};

use crate::store::{block::Block, fragment};

/// A segment of block headers.
pub type HeaderSegment = Segment<fragment::Slot<fragment::BlockHeader>>;

/// A segment of block transactions.
pub type TransactionSegment = Segment<fragment::Slot<Vec<fragment::Transaction>>>;

/// A segment of block validators.
pub type ValidatorSegment = Segment<fragment::Slot<Vec<fragment::Validator>>>;

/// A segment of block blobs.
pub type BlobSegment = Segment<fragment::Slot<Vec<fragment::Blob>>>;

pub struct SegmentBuilder {
    index: IndexSegment,
    header: HeaderSegment,
    transaction: TransactionSegment,
    validator: ValidatorSegment,
    blob: BlobSegment,
}

impl SegmentBuilder {
    pub fn new(first_block: &Cursor) -> Self {
        Self {
            index: IndexSegment::new(first_block.clone()),
            header: HeaderSegment::new(first_block.clone()),
            transaction: TransactionSegment::new(first_block.clone()),
            validator: ValidatorSegment::new(first_block.clone()),
            blob: BlobSegment::new(first_block.clone()),
        }
    }

    pub fn add_block(&mut self, block: fragment::Slot<Block>) {
        use fragment::Slot::*;

        match block {
            Missed { slot } => {
                self.index.push(IndexGroup::default());
                self.header.push(Missed { slot });
                self.transaction.push(Missed { slot });
                self.validator.push(Missed { slot });
                self.blob.push(Missed { slot });
            }
            Proposed(block) => {
                self.index.push(block.index);
                self.header.push(Proposed(block.header));
                self.transaction.push(Proposed(block.transactions));
                self.validator.push(Proposed(block.validators));
                self.blob.push(Proposed(block.blobs));
            }
        }
    }

    pub fn to_segment_data(&self) -> Result<Vec<SerializedSegment>, SegmentError> {
        let index = self
            .index
            .to_serialized_segment()
            .change_context(SegmentError)?;
        let header = self
            .header
            .to_serialized_segment()
            .change_context(SegmentError)?;
        let transaction = self
            .transaction
            .to_serialized_segment()
            .change_context(SegmentError)?;
        let validator = self
            .validator
            .to_serialized_segment()
            .change_context(SegmentError)?;
        let blob = self
            .blob
            .to_serialized_segment()
            .change_context(SegmentError)?;
        Ok(vec![index, header, transaction, validator, blob])
    }
}

impl Fragment for fragment::Slot<fragment::BlockHeader> {
    fn name() -> &'static str {
        "header"
    }
}

impl Fragment for fragment::Slot<Vec<fragment::Transaction>> {
    fn name() -> &'static str {
        "transaction"
    }
}

impl Fragment for fragment::Slot<Vec<fragment::Validator>> {
    fn name() -> &'static str {
        "validator"
    }
}

impl Fragment for fragment::Slot<Vec<fragment::Blob>> {
    fn name() -> &'static str {
        "blob"
    }
}
