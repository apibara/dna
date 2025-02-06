use error_stack::{Result, ResultExt};

use crate::fragment::{
    Block, BodyFragment, FragmentId, HeaderFragment, IndexFragment, JoinFragment,
};

use super::segment_access::{FileEntry, SegmentBlockAccess};

#[derive(Debug)]
pub struct FragmentAccessError;

pub struct BlockAccess(FileEntry);

pub enum FragmentAccess<'a> {
    Segment(SegmentBlockAccess<'a>),
    Block(BlockAccess),
}

impl<'a> FragmentAccess<'a> {
    pub fn get_index_fragment(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<IndexFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_index_fragment(fragment_id),
            Self::Block(access) => access.get_index_fragment(fragment_id),
        }
    }

    pub fn get_join_fragment(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<JoinFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_join_fragment(fragment_id),
            Self::Block(access) => access.get_join_fragment(fragment_id),
        }
    }

    pub fn get_header_fragment(
        &'a self,
    ) -> Result<&'a rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_header_fragment(),
            Self::Block(access) => access.get_header_fragment(),
        }
    }

    pub fn get_body_fragment(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<BodyFragment>, FragmentAccessError> {
        match self {
            Self::Segment(access) => access.get_body_fragment(fragment_id),
            Self::Block(access) => access.get_body_fragment(fragment_id),
        }
    }
}

impl BlockAccess {
    pub fn get_index_fragment<'a>(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<IndexFragment>, FragmentAccessError> {
        let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(self.0.value()) };

        let Some(pos) = block
            .index
            .indexes
            .iter()
            .position(|f| f.fragment_id == *fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("index for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block.index.indexes[pos])
    }

    pub fn get_join_fragment<'a>(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<JoinFragment>, FragmentAccessError> {
        let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(self.0.value()) };

        let Some(pos) = block
            .join
            .joins
            .iter()
            .position(|f| f.fragment_id == *fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("joins for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block.join.joins[pos])
    }

    pub fn get_header_fragment(
        &self,
    ) -> Result<&rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(self.0.value()) };
        Ok(&block.header)
    }

    pub fn get_body_fragment<'a>(
        &'a self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<BodyFragment>, FragmentAccessError> {
        let block = unsafe { rkyv::access_unchecked::<rkyv::Archived<Block>>(self.0.value()) };

        let Some(pos) = block
            .body
            .iter()
            .position(|f| f.fragment_id == *fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("body for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block.body[pos])
    }
}

impl error_stack::Context for FragmentAccessError {}

impl std::fmt::Display for FragmentAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fragment access error")
    }
}

impl From<FileEntry> for BlockAccess {
    fn from(value: FileEntry) -> Self {
        BlockAccess(value)
    }
}
