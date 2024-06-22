use apibara_dna_common::{
    core::Cursor, ingestion::SegmentBuilder, segment::store::Segment, storage::StorageBackend,
};
use error_stack::{Report, Result};

#[derive(Debug)]
pub struct BeaconChainSegmentBuilderError {}

pub struct BeaconChainSegmentBuilder {}

#[async_trait::async_trait]
impl SegmentBuilder for BeaconChainSegmentBuilder {
    type Error = BeaconChainSegmentBuilderError;

    fn create_segment(&mut self, cursors: &[Cursor]) {
        todo!()
    }

    async fn write_segment<S: StorageBackend>(
        &mut self,
        segment_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error> {
        todo!();
    }

    async fn write_segment_group<S: StorageBackend>(
        &mut self,
        segment_group_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error> {
        todo!();
    }

    fn cleanup_segment_data(&mut self, cursors: &[Cursor]) {
        todo!();
    }
}

impl error_stack::Context for BeaconChainSegmentBuilderError {}

impl std::fmt::Display for BeaconChainSegmentBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to build segment")
    }
}
