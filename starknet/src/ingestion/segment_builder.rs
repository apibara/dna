use apibara_dna_common::ingestion::{
    SegmentBuilder as SegmentBuilderTrait, SegmentData, SegmentGroupData,
};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};

use crate::segment::{store, SegmentBuilder, SegmentGroupBuilder};

pub struct StarknetSegmentBuilder {
    segment_builder: SegmentBuilder,
    segment_group_builder: SegmentGroupBuilder,
}

#[derive(Debug)]
pub struct StarknetSegmentBuilderError;

impl StarknetSegmentBuilder {
    pub fn new() -> Self {
        Self {
            segment_builder: SegmentBuilder::default(),
            segment_group_builder: SegmentGroupBuilder::default(),
        }
    }
}

#[async_trait]
impl SegmentBuilderTrait<store::SingleBlock> for StarknetSegmentBuilder {
    type Error = StarknetSegmentBuilderError;

    async fn add_block(&mut self, block: store::SingleBlock) -> Result<(), Self::Error> {
        self.segment_builder.add_single_block(block);
        Ok(())
    }

    async fn take_segment(&mut self) -> Result<Vec<SegmentData>, Self::Error> {
        let (index, segment_data) = self
            .segment_builder
            .take_segment_data()
            .change_context(StarknetSegmentBuilderError)?;
        self.segment_builder.reset();
        self.segment_group_builder.add_segment_index(index);
        Ok(segment_data)
    }

    async fn take_segment_group(&mut self) -> Result<SegmentGroupData, Self::Error> {
        let data = self
            .segment_group_builder
            .take_segment_group_data()
            .change_context(StarknetSegmentBuilderError)?;
        self.segment_group_builder.reset();
        Ok(data)
    }
}

impl error_stack::Context for StarknetSegmentBuilderError {}

impl std::fmt::Display for StarknetSegmentBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Starknet segment builder error")
    }
}
