use std::collections::HashMap;

use error_stack::{Result, ResultExt};

use crate::{
    file_cache::Mmap,
    store::{
        segment::{FragmentData, Segment, SerializedSegment},
        Block,
    },
    Cursor,
};

use super::CompactionError;

#[derive(Debug, Default)]
pub struct SegmentBuilder {
    expected_fragment_count: Option<usize>,
    first_block: Option<Cursor>,
    segments: HashMap<u8, (String, Vec<FragmentData>)>,
}

impl SegmentBuilder {
    pub fn start_new_segment(&mut self, first_block: Cursor) -> Result<(), CompactionError> {
        if self.first_block.is_some() {
            return Err(CompactionError)
                .attach_printable("segment builder already started a segment");
        }

        self.first_block = Some(first_block);

        Ok(())
    }

    pub fn add_block(&mut self, cursor: &Cursor, bytes: Mmap) -> Result<(), CompactionError> {
        let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&bytes)
            .change_context(CompactionError)
            .attach_printable("failed to access block")?;

        if let Some(expected_count) = self.expected_fragment_count {
            if block.fragments.len() != expected_count {
                return Err(CompactionError)
                    .attach_printable("block does not have the expected number of fragments")
                    .attach_printable("this is a bug in the network-specific ingestion code")
                    .attach_printable_lazy(|| format!("expected: {}", expected_count))
                    .attach_printable_lazy(|| format!("actual: {}", block.fragments.len()));
            }
        } else {
            self.expected_fragment_count = Some(block.fragments.len());
        }

        for fragment in block.fragments.iter() {
            let tag = fragment.tag;
            let name = fragment.name.to_string();
            let data = FragmentData {
                cursor: cursor.clone(),
                data: fragment.data.to_vec(),
            };

            if let Some(existing) = self.segments.get_mut(&tag) {
                existing.1.push(data);
            } else {
                self.segments.insert(tag, (name, vec![data]));
            }
        }

        Ok(())
    }

    pub fn segment_data(&mut self) -> Result<Vec<SerializedSegment>, CompactionError> {
        let Some(first_block) = self.first_block.take() else {
            return Err(CompactionError).attach_printable("no segment started");
        };

        let segments = std::mem::take(&mut self.segments);
        let mut serialized = Vec::with_capacity(segments.len());

        for (name, data) in segments.into_values() {
            let segment = Segment {
                first_block: first_block.clone(),
                data,
            };
            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&segment)
                .change_context(CompactionError)
                .attach_printable("failed to serialize segment")?;

            serialized.push(SerializedSegment { name, data });
        }

        // NOTE: we leave the expected_fragment_count field as is because we want the data to be consistent
        // across all segments.

        Ok(serialized)
    }
}
