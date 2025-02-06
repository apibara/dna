use std::collections::HashMap;

use bytes::Bytes;
use error_stack::{Result, ResultExt};
use foyer::CacheEntry;
use roaring::RoaringBitmap;

use crate::{
    core::Cursor,
    file_cache::{FileCacheError, FileFetch},
    fragment::{
        BodyFragment, FragmentId, HeaderFragment, IndexFragment, IndexGroupFragment, JoinFragment,
        JoinGroupFragment, HEADER_FRAGMENT_ID, INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID,
    },
    segment::Segment,
};

use super::fragment_access::FragmentAccessError;

pub type FileEntry = CacheEntry<String, Bytes>;

pub struct SegmentAccessFetch {
    first_block: u64,
    blocks: RoaringBitmap,
    fragments: HashMap<FragmentId, FileFetch>,
}

pub struct SegmentAccess {
    pub first_block: u64,
    pub blocks: RoaringBitmap,
    fragments: HashMap<FragmentId, FileEntry>,
}

pub struct SegmentAccessIter<'a> {
    segment: &'a SegmentAccess,
    inner: roaring::bitmap::Iter<'a>,
}

pub struct SegmentBlockAccess<'a> {
    offset: usize,
    segment: &'a SegmentAccess,
}

impl SegmentAccessFetch {
    pub fn new(first_block: u64, blocks: RoaringBitmap) -> Self {
        Self {
            first_block,
            blocks,
            fragments: HashMap::new(),
        }
    }

    pub fn insert_fragment(&mut self, fragment_id: FragmentId, fetch: FileFetch) {
        self.fragments.insert(fragment_id, fetch);
    }

    pub async fn wait(self) -> Result<SegmentAccess, FragmentAccessError> {
        let mut access = SegmentAccess {
            first_block: self.first_block,
            blocks: self.blocks,
            fragments: Default::default(),
        };

        for (fragment_id, fetch) in self.fragments.into_iter() {
            let file = fetch
                .await
                .map_err(FileCacheError::Foyer)
                .change_context(FragmentAccessError)?;
            access.fragments.insert(fragment_id, file);
        }

        Ok(access)
    }
}

impl SegmentAccess {
    pub fn fragment_len(&self) -> usize {
        self.fragments.len()
    }

    pub fn iter(&self) -> SegmentAccessIter<'_> {
        SegmentAccessIter {
            segment: self,
            inner: self.blocks.iter(),
        }
    }
}

impl<'a> SegmentBlockAccess<'a> {
    pub fn cursor(&self) -> Cursor {
        Cursor::new_finalized(self.block_number())
    }

    pub fn block_number(&self) -> u64 {
        self.segment.first_block + self.offset as u64
    }

    pub fn get_index_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<IndexFragment>, FragmentAccessError> {
        let entry = self
            .segment
            .fragments
            .get(&INDEX_FRAGMENT_ID)
            .ok_or(FragmentAccessError)
            .attach_printable("index fragment not found")?;

        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<IndexGroupFragment>>>(entry.value())
        };

        let block_index = &segment.data[self.offset];

        let Some(pos) = block_index
            .data
            .indexes
            .iter()
            .position(|f| f.fragment_id == *fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("index for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block_index.data.indexes[pos])
    }

    pub fn get_join_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<JoinFragment>, FragmentAccessError> {
        let entry = self
            .segment
            .fragments
            .get(&JOIN_FRAGMENT_ID)
            .ok_or(FragmentAccessError)
            .attach_printable("join fragment not found")?;

        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<JoinGroupFragment>>>(entry.value())
        };

        let block_index = &segment.data[self.offset];

        let Some(pos) = block_index
            .data
            .joins
            .iter()
            .position(|f| f.fragment_id == *fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("join for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block_index.data.joins[pos])
    }

    pub fn get_header_fragment(
        &self,
    ) -> Result<&'a rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        let entry = self
            .segment
            .fragments
            .get(&HEADER_FRAGMENT_ID)
            .ok_or(FragmentAccessError)
            .attach_printable("header fragment not found")?;

        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<HeaderFragment>>>(entry.value())
        };

        Ok(&segment.data[self.offset].data)
    }

    pub fn get_body_fragment(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<&'a rkyv::Archived<BodyFragment>, FragmentAccessError> {
        let entry = self
            .segment
            .fragments
            .get(fragment_id)
            .ok_or(FragmentAccessError)
            .attach_printable("body fragment not found")
            .attach_printable_lazy(|| format!("fragment id: {}", fragment_id))
            .attach_printable_lazy(|| format!("block number: {}", self.block_number()))?;

        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<BodyFragment>>>(entry.value())
        };

        Ok(&segment.data[self.offset].data)
    }
}

impl<'a> Iterator for SegmentAccessIter<'a> {
    type Item = SegmentBlockAccess<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let first_block = self.segment.first_block;
        let block_number = self.inner.next()? as u64;

        assert!(first_block <= block_number);

        let offset = block_number - first_block;

        Some(SegmentBlockAccess {
            segment: self.segment,
            offset: offset as usize,
        })
    }
}

impl std::fmt::Debug for SegmentBlockAccess<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentBlockAccess")
            .field("segment", &self.segment.first_block)
            .field("offset", &self.offset)
            .finish()
    }
}
