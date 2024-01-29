use std::io::Cursor;

use error_stack::ResultExt;
use flatbuffers::FlatBufferBuilder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    error::{DnaError, Result},
    storage::StorageBackend,
};

use super::{store, SegmentOptions};

pub struct SnapshotReader<S: StorageBackend> {
    storage: S,
    buffer: Vec<u8>,
}

impl<S> SnapshotReader<S>
where
    S: StorageBackend,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S) -> Self {
        let buffer = vec![0; 1024];
        Self { storage, buffer }
    }

    pub async fn read<'a>(&'a mut self) -> Result<store::Snapshot<'a>> {
        let mut reader = self.storage.get("", "snapshot").await?;
        let len = {
            let mut cursor = Cursor::new(&mut self.buffer[..]);
            tokio::io::copy(&mut reader, &mut cursor)
                .await
                .change_context(DnaError::Io)? as usize
        };

        let stored = flatbuffers::root::<store::Snapshot>(&self.buffer[..len])
            .change_context(DnaError::Storage)
            .attach_printable("failed to decode snapshot")?;

        Ok(stored)
    }

    pub async fn snapshot_state(&mut self) -> Result<SnapshotState> {
        let stored = self.read().await?;
        Ok(SnapshotState::from_snapshot(&stored))
    }
}

pub struct SnapshotBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    state: SnapshotState,
}

pub struct SnapshotState {
    pub revision: u64,
    pub first_block_number: u64,
    pub group_count: u32,
    pub segment_options: SegmentOptions,
}

impl<'a> SnapshotBuilder<'a> {
    pub fn new(starting_block: u64, segment_options: SegmentOptions) -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            state: SnapshotState {
                revision: 0,
                first_block_number: starting_block,
                group_count: 0,
                segment_options,
            },
        }
    }

    pub async fn from_storage<S>(storage: &mut S) -> Result<Option<SnapshotBuilder<'a>>>
    where
        S: StorageBackend,
        <S as StorageBackend>::Reader: Unpin,
    {
        let exists = storage.exists("", "snapshot").await?;
        if !exists {
            return Ok(None);
        }

        let mut reader = storage.get("", "snapshot").await?;
        let mut buf = Vec::new();
        reader
            .read_to_end(&mut buf)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to read snapshot content")?;

        let stored = flatbuffers::root::<store::Snapshot>(&buf)
            .change_context(DnaError::Storage)
            .attach_printable("failed to decode snapshot")?;

        let state = SnapshotState::from_snapshot(&stored);

        let builder = FlatBufferBuilder::new();
        Ok(Self { builder, state }.into())
    }

    pub async fn write_revision<S>(&mut self, storage: &mut S) -> Result<u64>
    where
        S: StorageBackend,
        <S as StorageBackend>::Writer: Unpin,
    {
        self.state.revision += 1;
        self.state.group_count += 1;

        let snapshot = store::Snapshot::create(
            &mut self.builder,
            &store::SnapshotArgs {
                first_block_number: self.state.first_block_number,
                revision: self.state.revision,
                group_count: self.state.group_count,
                segment_size: self.state.segment_options.segment_size as u32,
                group_size: self.state.segment_options.group_size as u32,
                target_num_digits: self.state.segment_options.target_num_digits as u32,
            },
        );
        self.builder.finish(snapshot, None);

        let mut writer = storage.put("", "snapshot").await?;
        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        Ok(self.state.revision)
    }

    pub fn reset(&mut self) {
        self.builder.reset();
    }

    pub fn state(&self) -> &SnapshotState {
        &self.state
    }
}

impl SnapshotState {
    pub fn from_snapshot(snapshot: &store::Snapshot) -> Self {
        Self {
            revision: snapshot.revision(),
            first_block_number: snapshot.first_block_number(),
            group_count: snapshot.group_count(),
            segment_options: SegmentOptions {
                segment_size: snapshot.segment_size() as usize,
                group_size: snapshot.group_size() as usize,
                target_num_digits: snapshot.target_num_digits() as usize,
            },
        }
    }

    pub fn starting_block(&self) -> u64 {
        let ingested_blocks = self.group_count as u64 * self.segment_options.segment_group_blocks();
        self.first_block_number + ingested_blocks
    }
}
