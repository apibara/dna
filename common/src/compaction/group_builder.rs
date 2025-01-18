use std::collections::BTreeMap;

use error_stack::{Result, ResultExt};

use crate::{
    fragment::{self, FragmentId, IndexFragment, IndexGroupFragment, IndexId},
    index,
    segment::{Segment, SegmentGroup},
    Cursor,
};

use super::CompactionError;

#[derive(Debug)]
pub struct SegmentGroupBuilder {
    segment_size: u64,
    block_range: Option<(Cursor, u64)>,
    block_indexes: BTreeMap<FragmentId, BTreeMap<IndexId, index::BitmapIndexBuilder>>,
}

impl SegmentGroupBuilder {
    pub fn new(segment_size: usize) -> Self {
        Self {
            segment_size: segment_size as u64,
            block_range: None,
            block_indexes: BTreeMap::new(),
        }
    }

    pub fn add_segment(
        &mut self,
        segment: &Segment<IndexGroupFragment>,
    ) -> Result<(), CompactionError> {
        let segment_cursor = segment.first_block.clone();
        let segment_end = segment_cursor.number + self.segment_size - 1;

        self.block_range = match self.block_range.take() {
            None => Some((segment_cursor, segment_end)),
            Some((first_block, _)) => Some((first_block, segment_end)),
        };

        for block_data in segment.data.iter() {
            let cursor: Cursor = block_data.cursor.clone();
            let block_number = cursor.number as u32;

            for index_fragment in block_data.data.indexes.iter() {
                let block_index_fragment = self
                    .block_indexes
                    .entry(index_fragment.fragment_id)
                    .or_default();

                for index in index_fragment.indexes.iter() {
                    let block_index = block_index_fragment.entry(index.index_id).or_default();
                    match &index.index {
                        index::Index::Bitmap(bitmap_index) => {
                            for key in bitmap_index.keys() {
                                block_index.insert(key.clone(), block_number);
                            }
                        }
                        index::Index::Empty => {}
                    }
                }
            }
        }

        Ok(())
    }

    pub fn build(self) -> Result<SegmentGroup, CompactionError> {
        let Some((first_block, last_block)) = self.block_range else {
            return Err(CompactionError).attach_printable("segment group builder has no segments");
        };

        let range_start = first_block.number as u32;
        let range_len = (last_block - first_block.number + 1) as u32;

        let mut indexes = Vec::new();

        for (fragment_id, fragment_indexes) in self.block_indexes.into_iter() {
            let fragment_indexes = fragment_indexes
                .into_iter()
                .map(|(index_id, index_builder)| {
                    let index = index_builder.build().change_context(CompactionError)?;
                    Ok(fragment::Index {
                        index_id,
                        index: index.into(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            indexes.push(IndexFragment {
                fragment_id,
                range_start,
                range_len,
                indexes: fragment_indexes,
            })
        }

        indexes.sort_by_key(|index| index.fragment_id);

        let index = IndexGroupFragment { indexes };

        Ok(SegmentGroup { first_block, index })
    }
}
