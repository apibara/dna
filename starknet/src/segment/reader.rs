use apibara_dna_common::{
    error::{DnaError, Result},
    segment::SegmentOptions,
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use error_stack::ResultExt;

use super::store;

#[derive(Debug, Copy, Clone)]
pub struct SegmentReaderOptions {
    pub needs_receipts: bool,
    pub needs_transactions: bool,
    pub needs_events: bool,
}

pub struct SegmentReader<S: StorageBackend + Send> {
    storage: CachedStorage<S>,
    local_storage: LocalStorageBackend,
    segment_options: SegmentOptions,
    options: SegmentReaderOptions,
}

impl<S> SegmentReader<S>
where
    S: StorageBackend + Send,
    S::Reader: Unpin + Send,
{
    pub fn new(
        storage: CachedStorage<S>,
        local_storage: LocalStorageBackend,
        segment_options: SegmentOptions,
        options: SegmentReaderOptions,
    ) -> Self {
        Self {
            storage,
            local_storage,
            segment_options,
            options,
        }
    }

    pub async fn segment_group(&mut self, block_number: u64) -> Result<store::SegmentGroup> {
        let segment_group_name = self.segment_options.format_segment_group_name(block_number);

        let bytes = self.storage.mmap("group", segment_group_name).await?;

        let segment_group = rkyv::from_bytes::<store::SegmentGroup>(&bytes)
            .map_err(|_| DnaError::Io)
            .attach_printable("failed to deserialize segment group")?;

        Ok(segment_group)
    }

    pub async fn header(&mut self, block_number: u64) -> Result<store::BlockHeaderSegment> {
        let segment_name = self.segment_options.format_segment_name(block_number);
        let bytes = self
            .storage
            .mmap(format!("segment/{segment_name}"), "header")
            .await?;
        let segment = rkyv::from_bytes::<store::BlockHeaderSegment>(&bytes)
            .map_err(|_| DnaError::Io)
            .attach_printable("failed to deserialize block header")?;

        Ok(segment)
    }
}

impl SegmentReaderOptions {
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            needs_receipts: self.needs_receipts || other.needs_receipts,
            needs_transactions: self.needs_transactions || other.needs_transactions,
            needs_events: self.needs_events || other.needs_events,
        }
    }
}

impl Default for SegmentReaderOptions {
    fn default() -> Self {
        Self {
            needs_receipts: false,
            needs_transactions: false,
            needs_events: false,
        }
    }
}
