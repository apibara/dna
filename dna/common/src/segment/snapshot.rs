use error_stack::ResultExt;
use tokio::io::AsyncReadExt;

use crate::{
    error::{DnaError, Result},
    storage::StorageBackend,
};

use super::{store, SegmentOptions};

pub struct SnapshotBuilder<S>
where
    S: StorageBackend,
    S::Reader: Unpin,
{
    storage: S,
    segment_options: SegmentOptions,
    revision: u64,
    first_block_number: u64,
    group_count: usize,
}

pub struct Snapshot {
    state: SnapshotState,
}

pub struct SnapshotState {
    pub revision: u64,
    pub first_block_number: u64,
    pub segment_size: usize,
    pub group_size: usize,
    pub group_count: usize,
}

impl<S> SnapshotBuilder<S>
where
    S: StorageBackend,
    S::Reader: Unpin,
{
    pub fn new(storage: S, options: SegmentOptions) -> Self {
        SnapshotBuilder {
            storage,
            first_block_number: 0,
            revision: 0,
            group_count: 0,
            segment_options: options,
        }
    }

    pub fn with_first_block_number(mut self, first_block_number: u64) -> Self {
        self.first_block_number = first_block_number;
        self
    }

    pub async fn merge_with_storage(mut self) -> Result<Self> {
        let exists = self.storage.exists("", "snapshot").await?;
        if !exists {
            return Ok(self);
        }

        let mut reader = self.storage.get("", "snapshot").await?;
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read snapshot content")?;

        let stored = flatbuffers::root::<store::Snapshot>(&buf)
            .change_context(DnaError::Storage)
            .attach_printable("failed to decode snapshot")?;

        self.revision = stored.revision();
        self.first_block_number = stored.first_block_number();
        self.group_count = stored.group_count() as usize;
        self.segment_options = SegmentOptions {
            segment_size: stored.segment_size() as usize,
            group_size: stored.group_size() as usize,
            ..self.segment_options
        };

        Ok(self)
    }

    pub fn build(self) -> Snapshot {
        Snapshot {
            state: SnapshotState {
                revision: self.revision,
                first_block_number: self.first_block_number,
                segment_size: self.segment_options.segment_size,
                group_size: self.segment_options.group_size,
                group_count: self.group_count,
            },
        }
    }
}

impl Snapshot {
    pub fn state(&self) -> &SnapshotState {
        &self.state
    }
}
