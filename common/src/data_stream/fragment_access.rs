use error_stack::{Result, ResultExt};

use crate::{
    block_store::BlockStoreReader,
    core::Cursor,
    file_cache::{CachedFile, FileCacheError},
    fragment::{
        Block, BodyFragment, FragmentId, HeaderFragment, IndexFragment, IndexGroupFragment,
        JoinFragment, JoinGroupFragment, HEADER_FRAGMENT_ID, HEADER_FRAGMENT_NAME,
        INDEX_FRAGMENT_NAME, JOIN_FRAGMENT_NAME,
    },
    segment::Segment,
};

use super::segment_access::SegmentBlockAccess;

#[derive(Debug)]
pub struct FragmentAccessError;

pub enum FragmentAccess<'a> {
    Segment(SegmentBlockAccess<'a>),
}

impl<'a> FragmentAccess<'a> {
    pub fn get_index_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<IndexFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_index_fragment(fragment_id),
        }
    }

    pub fn get_join_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<JoinFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_join_fragment(fragment_id),
        }
    }

    pub fn get_header_fragment(
        &self,
    ) -> Result<&'a rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_header_fragment(),
        }
    }

    pub fn get_body_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<BodyFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_body_fragment(fragment_id),
        }
    }
}

impl error_stack::Context for FragmentAccessError {}

impl std::fmt::Display for FragmentAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fragment access error")
    }
}
