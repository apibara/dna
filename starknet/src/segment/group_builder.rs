use apibara_dna_common::ingestion::SegmentGroupData;
use error_stack::{Result, ResultExt};

use crate::{ingestion::StarknetSegmentBuilderError, segment::store};

use super::index::Index;

#[derive(Default)]
pub struct SegmentGroupBuilder {
    index: Index,
}

impl SegmentGroupBuilder {
    pub fn reset(&mut self) {
        self.index = Index::default();
    }

    pub fn add_segment_index(&mut self, index: Index) {
        self.index.merge(index);
    }

    pub fn take_segment_group_data(
        &mut self,
    ) -> Result<SegmentGroupData, StarknetSegmentBuilderError> {
        let index = store::Index::try_from(std::mem::take(&mut self.index))
            .change_context(StarknetSegmentBuilderError)
            .attach_printable("failed to convert segment group index")?;
        let segment_group = store::SegmentGroup { index };
        let bytes =
            rkyv::to_bytes::<_, 0>(&segment_group).change_context(StarknetSegmentBuilderError)?;

        self.reset();

        Ok(SegmentGroupData {
            data: bytes.to_vec(),
        })
    }
}
