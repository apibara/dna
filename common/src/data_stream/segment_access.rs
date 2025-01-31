use std::collections::HashMap;

use bytes::Bytes;
use error_stack::{Result, ResultExt};
use foyer::{CacheEntry, HybridFetch};

use crate::{
    file_cache::FileCacheError,
    fragment::{
        BodyFragment, FragmentId, HeaderFragment, IndexFragment, IndexGroupFragment, JoinFragment,
        JoinGroupFragment,
    },
    segment::Segment,
};

use super::fragment_access::FragmentAccessError;

pub struct SegmentAccessFetch {
    pub index: HybridFetch<String, Bytes>,
    pub joins: HybridFetch<String, Bytes>,
    pub header: HybridFetch<String, Bytes>,
    pub body: Vec<(FragmentId, HybridFetch<String, Bytes>)>,
}

pub struct SegmentAccess {
    offset: usize,
    index_entry: CacheEntry<String, Bytes>,
    joins_entry: CacheEntry<String, Bytes>,
    header_entry: CacheEntry<String, Bytes>,
    body_entries: HashMap<FragmentId, CacheEntry<String, Bytes>>,
}

impl SegmentAccessFetch {
    pub async fn wait_all_segments(mut self) -> Result<SegmentAccess, FragmentAccessError> {
        let index = self
            .index
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(FragmentAccessError)?;

        let joins = self
            .joins
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(FragmentAccessError)?;

        let header = self
            .header
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(FragmentAccessError)?;

        let mut body = HashMap::new();
        for (fragment_id, body_fetch) in self.body.iter_mut() {
            let entry = body_fetch
                .await
                .map_err(FileCacheError::Foyer)
                .change_context(FragmentAccessError)?;
            body.insert(*fragment_id, entry);
        }

        Ok(SegmentAccess {
            offset: 0,
            index_entry: index,
            joins_entry: joins,
            header_entry: header,
            body_entries: body,
        })
    }
}

impl SegmentAccess {
    pub fn get_fragment_index(
        &self,
        fragment_id: FragmentId,
    ) -> Result<&rkyv::Archived<IndexFragment>, FragmentAccessError> {
        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<IndexGroupFragment>>>(
                self.index_entry.value(),
            )
        };

        let block_index = &segment.data[self.offset];

        let Some(pos) = block_index
            .data
            .indexes
            .iter()
            .position(|f| f.fragment_id == fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("joins for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block_index.data.indexes[pos])
    }

    pub fn get_fragment_joins(
        &self,
        fragment_id: FragmentId,
    ) -> Result<&rkyv::Archived<JoinFragment>, FragmentAccessError> {
        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<JoinGroupFragment>>>(
                self.joins_entry.value(),
            )
        };

        let block_index = &segment.data[self.offset];

        let Some(pos) = block_index
            .data
            .joins
            .iter()
            .position(|f| f.fragment_id == fragment_id)
        else {
            return Err(FragmentAccessError)
                .attach_printable("joins for fragment not found")
                .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
        };

        Ok(&block_index.data.joins[pos])
    }

    pub fn get_header(&self) -> Result<&rkyv::Archived<HeaderFragment>, FragmentAccessError> {
        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<HeaderFragment>>>(
                self.header_entry.value(),
            )
        };

        Ok(&segment.data[self.offset].data)
    }

    pub fn get_body(
        &self,
        fragment_id: FragmentId,
    ) -> Result<&rkyv::Archived<BodyFragment>, FragmentAccessError> {
        let entry = self
            .body_entries
            .get(&fragment_id)
            .ok_or(FragmentAccessError)
            .attach_printable("body for fragment not found")
            .attach_printable_lazy(|| format!("fragment id: {}", fragment_id))?;

        let segment = unsafe {
            rkyv::access_unchecked::<rkyv::Archived<Segment<BodyFragment>>>(entry.value())
        };

        Ok(&segment.data[self.offset].data)
    }
}
