use error_stack::{Result, ResultExt};
use rkyv::{util::AlignedVec, Archive, Deserialize, Portable, Serialize};

use crate::{
    rkyv::{Aligned, Checked},
    Cursor,
};

use super::fragment::{Fragment, FragmentError};

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct FragmentData {
    pub cursor: Cursor,
    #[rkyv(with = Aligned<16>)]
    pub data: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct Segment {
    /// The first block in the segment.
    pub first_block: Cursor,
    /// The segment body.
    pub data: Vec<FragmentData>,
}

/// A segment ready to be written to the storage.
pub struct SerializedSegment {
    pub name: String,
    pub data: AlignedVec,
}

impl FragmentData {
    pub fn access_fragment<F>(&self) -> Result<&<F as Archive>::Archived, FragmentError>
    where
        F: Fragment,
        F::Archived: Portable + for<'a> Checked<'a>,
    {
        rkyv::access::<rkyv::Archived<F>, rkyv::rancor::Error>(&self.data)
            .change_context(FragmentError)
            .attach_printable("failed to access fragment")
            .attach_printable_lazy(|| format!("fragment: {}", F::name()))
    }
}
