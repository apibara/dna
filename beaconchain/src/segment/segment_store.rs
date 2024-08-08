use apibara_dna_common::Cursor;
use error_stack::{Result, ResultExt};
use rkyv::{ser::serializers::AllocSerializer, AlignedVec, Archive, Deserialize, Serialize};

use crate::store::fragment;

#[derive(Debug)]
pub struct SegmentError;

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentBlock<TD, TI> {
    pub cursor: Cursor,
    pub index: TI,
    pub data: TD,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Segment<TD, TI> {
    /// The first block number in the segment.
    first_block_number: u64,
    /// The segment body.
    body: Vec<fragment::Slot<SegmentBlock<TD, TI>>>,
}

pub trait SegmentName {
    fn name(&self) -> &'static str;
}

/// A segment ready to be written to the storage.
pub struct SerializedSegment {
    pub name: String,
    pub data: AlignedVec,
}

impl<TD, TI> Segment<TD, TI> {
    pub fn new(first_block_number: u64) -> Self {
        Self {
            first_block_number,
            body: Vec::new(),
        }
    }

    pub fn push(&mut self, block: fragment::Slot<SegmentBlock<TD, TI>>) {
        self.body.push(block);
    }
}

impl<TD, TI> Segment<TD, TI>
where
    Self: SegmentName,
    TD: Serialize<AllocSerializer<0>>,
    TI: Serialize<AllocSerializer<0>>,
{
    pub fn to_serialized(&self) -> Result<SerializedSegment, SegmentError> {
        let data = rkyv::to_bytes::<_, 0>(self)
            .change_context(SegmentError)
            .attach_printable("failed to serialize segment")?;
        let name = self.name().to_string();
        Ok(SerializedSegment { name, data })
    }
}

impl error_stack::Context for SegmentError {}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment error")
    }
}
