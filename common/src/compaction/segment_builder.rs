use std::collections::HashMap;

use bytes::Bytes;
use error_stack::{Result, ResultExt};

use crate::{
    fragment::{
        Block, BodyFragment, HeaderFragment, IndexGroupFragment, JoinGroupFragment,
        HEADER_FRAGMENT_NAME, INDEX_FRAGMENT_NAME, JOIN_FRAGMENT_NAME,
    },
    segment::{FragmentData, Segment, SerializedSegment},
    Cursor,
};

use super::CompactionError;

#[derive(Debug, Default)]
pub struct SegmentBuilder {
    expected_fragment_count: Option<usize>,
    first_block: Option<Cursor>,
    headers: Vec<FragmentData<HeaderFragment>>,
    indexes: Vec<FragmentData<IndexGroupFragment>>,
    joins: Vec<FragmentData<JoinGroupFragment>>,
    body: HashMap<u8, (String, Vec<FragmentData<BodyFragment>>)>,
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

    pub fn block_count(&self) -> usize {
        self.headers.len()
    }

    pub fn add_block(&mut self, cursor: &Cursor, bytes: &Bytes) -> Result<(), CompactionError> {
        let block = rkyv::from_bytes::<Block, rkyv::rancor::Error>(bytes)
            .change_context(CompactionError)
            .attach_printable("failed to access block")?;

        if let Some(expected_count) = self.expected_fragment_count {
            if block.body.len() != expected_count {
                return Err(CompactionError)
                    .attach_printable("block does not have the expected number of fragments")
                    .attach_printable("this is a bug in the network-specific ingestion code")
                    .attach_printable_lazy(|| format!("expected: {}", expected_count))
                    .attach_printable_lazy(|| format!("actual: {}", block.body.len()));
            }
        } else {
            self.expected_fragment_count = Some(block.body.len());
        }

        {
            let index = block.index;

            let fragment_data = FragmentData {
                cursor: cursor.clone(),
                data: index,
            };

            self.indexes.push(fragment_data);
        }

        {
            let join = block.join;

            let fragment_data = FragmentData {
                cursor: cursor.clone(),
                data: join,
            };

            self.joins.push(fragment_data);
        }

        {
            let header = block.header;

            let fragment_data = FragmentData {
                cursor: cursor.clone(),
                data: header,
            };

            self.headers.push(fragment_data);
        }

        for fragment in block.body.into_iter() {
            let fragment_id = fragment.fragment_id;
            let name = fragment.name.to_string();

            let data = FragmentData {
                cursor: cursor.clone(),
                data: fragment,
            };

            if let Some(existing) = self.body.get_mut(&fragment_id) {
                existing.1.push(data);
            } else {
                self.body.insert(fragment_id, (name, vec![data]));
            }
        }

        Ok(())
    }

    pub fn segment_data(&mut self) -> Result<Vec<SerializedSegment>, CompactionError> {
        let Some(first_block) = self.first_block.take() else {
            return Err(CompactionError).attach_printable("no segment started");
        };

        let indexes = std::mem::take(&mut self.indexes);
        let joins = std::mem::take(&mut self.joins);
        let headers = std::mem::take(&mut self.headers);
        let segments = std::mem::take(&mut self.body);
        let expected_fragment_count = headers.len();

        let mut serialized = Vec::with_capacity(segments.len());

        if indexes.len() != headers.len() {
            return Err(CompactionError)
                .attach_printable("index, header, and body fragments do not match")
                .attach_printable_lazy(|| format!("indexes len: {}", indexes.len()))
                .attach_printable_lazy(|| format!("headers len: {}", headers.len()));
        }

        if joins.len() != headers.len() {
            return Err(CompactionError)
                .attach_printable("join, header, and body fragments do not match")
                .attach_printable_lazy(|| format!("joins len: {}", joins.len()))
                .attach_printable_lazy(|| format!("headers len: {}", headers.len()));
        }

        {
            let segment = Segment {
                first_block: first_block.clone(),
                data: indexes,
            };

            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&segment)
                .change_context(CompactionError)
                .attach_printable("failed to serialize index segment")?;
            let data = Bytes::copy_from_slice(data.as_slice());

            serialized.push(SerializedSegment {
                name: INDEX_FRAGMENT_NAME.to_string(),
                data,
            });
        }

        {
            let segment = Segment {
                first_block: first_block.clone(),
                data: joins,
            };

            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&segment)
                .change_context(CompactionError)
                .attach_printable("failed to serialize join segment")?;
            let data = Bytes::copy_from_slice(data.as_slice());

            serialized.push(SerializedSegment {
                name: JOIN_FRAGMENT_NAME.to_string(),
                data,
            });
        }

        {
            let segment = Segment {
                first_block: first_block.clone(),
                data: headers,
            };

            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&segment)
                .change_context(CompactionError)
                .attach_printable("failed to serialize header segment")?;
            let data = Bytes::copy_from_slice(data.as_slice());

            serialized.push(SerializedSegment {
                name: HEADER_FRAGMENT_NAME.to_string(),
                data,
            });
        }

        for (name, data) in segments.into_values() {
            if data.len() != expected_fragment_count {
                return Err(CompactionError)
                    .attach_printable("body fragments do not match")
                    .attach_printable_lazy(|| format!("expected: {}", expected_fragment_count))
                    .attach_printable_lazy(|| format!("actual: {}", data.len()));
            }

            let segment = Segment {
                first_block: first_block.clone(),
                data,
            };

            let data = rkyv::to_bytes::<rkyv::rancor::Error>(&segment)
                .change_context(CompactionError)
                .attach_printable("failed to serialize segment")?;
            let data = Bytes::copy_from_slice(data.as_slice());

            serialized.push(SerializedSegment { name, data });
        }

        // NOTE: we leave the expected_fragment_count field as is because we want the data to be consistent
        // across all segments.

        Ok(serialized)
    }
}
