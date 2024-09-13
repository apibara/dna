use error_stack::{Result, ResultExt};
use rkyv::{util::AlignedVec, Archive, Deserialize, Serialize};

use crate::{rkyv::Serializable, Cursor};

use super::index::IndexGroup;

/// A segment error.
#[derive(Debug)]
pub struct SegmentError;

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct SegmentBlock<TD> {
    pub cursor: Cursor,
    pub data: TD,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Segment<TD> {
    /// The first block in the segment.
    pub first_block: Cursor,
    /// The segment body.
    pub blocks: Vec<SegmentBlock<TD>>,
}

/// A fragment of data.
pub trait Fragment {
    fn name() -> &'static str;
}

/// A segment with only indices.
pub type IndexSegment = Segment<IndexGroup>;

/// A segment ready to be written to the storage.
pub struct SerializedSegment {
    pub name: String,
    pub data: AlignedVec,
}

impl<TD> Segment<TD> {
    pub fn new(first_block: Cursor) -> Self {
        Self {
            first_block,
            blocks: Vec::new(),
        }
    }

    pub fn push(&mut self, cursor: Cursor, block: TD) {
        self.blocks.push(SegmentBlock {
            cursor,
            data: block,
        });
    }
}

impl<TD> Segment<TD>
where
    TD: Fragment + for<'a> Serializable<'a>,
{
    pub fn to_serialized_segment(&self) -> Result<SerializedSegment, SegmentError> {
        let data = rkyv::to_bytes::<rkyv::rancor::Error>(self).change_context(SegmentError)?;
        let name = TD::name().to_string();
        Ok(SerializedSegment { name, data })
    }
}

impl error_stack::Context for SegmentError {}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment error")
    }
}

impl Fragment for IndexGroup {
    fn name() -> &'static str {
        "index"
    }
}
