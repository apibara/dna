use apibara_dna_common::{
    compaction::{CompactionError, SegmentBuilder},
    file_cache::Mmap,
    store::{
        index::IndexGroup,
        segment::{Fragment, IndexSegment, Segment, SerializedSegment},
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

#[derive(Default)]
pub struct BeaconChainSegmentBuilder {
    state: Option<BuilderState>,
}

struct BuilderState {
    index: IndexSegment,
    header: HeaderSegment,
    transaction: TransactionSegment,
    validator: ValidatorSegment,
    blob: BlobSegment,
}

impl SegmentBuilder for BeaconChainSegmentBuilder {
    fn start_new_segment(&mut self, first_block: Cursor) -> Result<(), CompactionError> {
        let state = BuilderState {
            index: IndexSegment::new(first_block.clone()),
            header: HeaderSegment::new(first_block.clone()),
            transaction: TransactionSegment::new(first_block.clone()),
            validator: ValidatorSegment::new(first_block.clone()),
            blob: BlobSegment::new(first_block.clone()),
        };
        self.state = Some(state);

        Ok(())
    }

    fn add_block(&mut self, cursor: &Cursor, bytes: Mmap) -> Result<(), CompactionError> {
        use fragment::Slot::*;
        let Some(state) = self.state.as_mut() else {
            return Err(CompactionError)
                .attach_printable("no segment state")
                .attach_printable("start a new segment first");
        };

        let block = rkyv::from_bytes::<fragment::Slot<Block>, rkyv::rancor::Error>(&bytes)
            .change_context(CompactionError)
            .attach_printable("failed to deserialize block")?;

        match block {
            Missed { slot } => {
                state.index.push(cursor.clone(), IndexGroup::default());
                state.header.push(cursor.clone(), Missed { slot });
                state.transaction.push(cursor.clone(), Missed { slot });
                state.validator.push(cursor.clone(), Missed { slot });
                state.blob.push(cursor.clone(), Missed { slot });
            }
            Proposed(block) => {
                state.index.push(cursor.clone(), block.index);
                state.header.push(cursor.clone(), Proposed(block.header));
                state
                    .transaction
                    .push(cursor.clone(), Proposed(block.transactions));
                state
                    .validator
                    .push(cursor.clone(), Proposed(block.validators));
                state.blob.push(cursor.clone(), Proposed(block.blobs));
            }
        }

        Ok(())
    }

    fn segment_data(&mut self) -> Result<Vec<SerializedSegment>, CompactionError> {
        let Some(state) = self.state.as_mut() else {
            return Err(CompactionError)
                .attach_printable("no segment state")
                .attach_printable("start a new segment first");
        };

        let index = state
            .index
            .to_serialized_segment()
            .change_context(CompactionError)?;
        let header = state
            .header
            .to_serialized_segment()
            .change_context(CompactionError)?;
        let transaction = state
            .transaction
            .to_serialized_segment()
            .change_context(CompactionError)?;
        let validator = state
            .validator
            .to_serialized_segment()
            .change_context(CompactionError)?;
        let blob = state
            .blob
            .to_serialized_segment()
            .change_context(CompactionError)?;

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

impl Clone for BeaconChainSegmentBuilder {
    fn clone(&self) -> Self {
        Self { state: None }
    }
}
