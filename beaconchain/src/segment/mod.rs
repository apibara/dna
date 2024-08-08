mod segment_store;

use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Serialize};

use crate::store::{self, fragment};

use self::segment_store::{Segment, SegmentBlock, SegmentError, SegmentName, SerializedSegment};

pub type HeaderSegment = Segment<fragment::BlockHeader, ()>;
pub type TransactionSegment = Segment<Vec<fragment::Transaction>, store::block::TransactionIndex>;
pub type ValidatorSegment = Segment<Vec<fragment::Validator>, store::block::ValidatorIndex>;
pub type BlobSegment = Segment<Vec<fragment::Blob>, store::block::BlobIndex>;

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentBuilder {
    header: HeaderSegment,
    transaction: TransactionSegment,
    validator: ValidatorSegment,
    blob: BlobSegment,
}

impl SegmentBuilder {
    pub fn new(first_block_number: u64) -> Self {
        Self {
            header: HeaderSegment::new(first_block_number),
            transaction: TransactionSegment::new(first_block_number),
            validator: ValidatorSegment::new(first_block_number),
            blob: BlobSegment::new(first_block_number),
        }
    }

    pub fn add_block(&mut self, block: store::fragment::Slot<store::block::IndexedBlock>) {
        match block {
            store::fragment::Slot::Missed => {
                self.header.push(store::fragment::Slot::Missed);
            }
            store::fragment::Slot::Proposed(block) => {
                let cursor = block.block.cursor();

                let header = SegmentBlock {
                    cursor: cursor.clone(),
                    index: (),
                    data: block.block.header,
                };
                self.header.push(store::fragment::Slot::Proposed(header));

                let transaction = SegmentBlock {
                    cursor: cursor.clone(),
                    index: block.transaction,
                    data: block.block.transactions,
                };
                self.transaction
                    .push(store::fragment::Slot::Proposed(transaction));

                let validator = SegmentBlock {
                    cursor: cursor.clone(),
                    index: block.validator,
                    data: block.block.validators,
                };
                self.validator
                    .push(store::fragment::Slot::Proposed(validator));

                let blob = SegmentBlock {
                    cursor: cursor.clone(),
                    index: block.blob,
                    data: block.block.blobs,
                };
                self.blob.push(store::fragment::Slot::Proposed(blob));
            }
        }
    }

    pub fn to_segment_data(&self) -> Result<Vec<SerializedSegment>, SegmentError> {
        let header = self.header.to_serialized().change_context(SegmentError)?;
        let transaction = self
            .transaction
            .to_serialized()
            .change_context(SegmentError)?;
        let validator = self
            .validator
            .to_serialized()
            .change_context(SegmentError)?;
        let blob = self.blob.to_serialized().change_context(SegmentError)?;

        Ok(vec![header, transaction, validator, blob])
    }
}

impl SegmentName for HeaderSegment {
    fn name(&self) -> &'static str {
        "header"
    }
}

impl SegmentName for TransactionSegment {
    fn name(&self) -> &'static str {
        "transaction"
    }
}
impl SegmentName for ValidatorSegment {
    fn name(&self) -> &'static str {
        "validator"
    }
}
impl SegmentName for BlobSegment {
    fn name(&self) -> &'static str {
        "blob"
    }
}
