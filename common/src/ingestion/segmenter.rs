use std::{collections::VecDeque, marker::PhantomData};

use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::{Stream, StreamExt};
use tokio::{io::AsyncWriteExt, sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    core::Cursor,
    ingestion::Snapshot,
    segment::SegmentOptions,
    storage::{segment_prefix, LocalStorageBackend, StorageBackend},
};

use super::{BlockIngestionEvent, SnapshotChange, SnapshotManager};

pub struct SegmentData {
    pub filename: String,
    pub data: Vec<u8>,
}

pub struct SegmentGroupData {
    pub data: Vec<u8>,
}

#[async_trait]
pub trait SegmentBuilder<T: rkyv::Archive> {
    type Error: error_stack::Context;

    async fn add_block(&mut self, block: T) -> Result<(), Self::Error>;

    async fn take_segment(&mut self) -> Result<Vec<SegmentData>, Self::Error>;

    async fn take_segment_group(&mut self) -> Result<SegmentGroupData, Self::Error>;
}

#[derive(Debug)]
pub enum SegmenterError {
    Initialization,
    UnexpectedEvent,
    Storage,
    Snapshot,
    StreamClosed,
    InvalidState,
}

#[derive(Debug, Clone)]
pub struct SegmenterOptions {
    pub channel_size: usize,
}

pub struct Segmenter<T, B, S, I>
where
    T: rkyv::Archive,
    B: SegmentBuilder<T> + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockIngestionEvent> + Unpin + Send + Sync + 'static,
{
    segment_builder: B,
    local_storage: LocalStorageBackend,
    storage: S,
    segment_options: SegmentOptions,
    snapshot_manager: SnapshotManager<S>,
    ingestion_stream: Option<I>,
    options: SegmenterOptions,
    snapshot: Snapshot,
    head: Cursor,
    finalized: Cursor,
    current: Cursor,
    block_count: usize,
    segment_count: usize,
    message_queue: VecDeque<SnapshotChange>,
    _block_phantom: std::marker::PhantomData<T>,
}

impl<T, B, S, I> Segmenter<T, B, S, I>
where
    T: rkyv::Archive + Send + Sync + 'static,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
    B: SegmentBuilder<T> + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
    I: Stream<Item = BlockIngestionEvent> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        segment_builder: B,
        segment_options: SegmentOptions,
        local_storage: LocalStorageBackend,
        storage: S,
        snapshot_manager: SnapshotManager<S>,
        ingestion_stream: I,
        options: SegmenterOptions,
    ) -> Self {
        Self {
            segment_builder,
            segment_options: segment_options.clone(),
            local_storage,
            storage,
            snapshot_manager,
            ingestion_stream: ingestion_stream.into(),
            options,
            snapshot: Snapshot::with_options(segment_options),
            head: Cursor::new_finalized(0),
            finalized: Cursor::new_finalized(0),
            current: Cursor::new_finalized(0),
            block_count: 0,
            segment_count: 0,
            message_queue: VecDeque::new(),
            _block_phantom: PhantomData,
        }
    }

    pub fn start(
        self,
        ct: CancellationToken,
    ) -> (
        impl Stream<Item = SnapshotChange>,
        JoinHandle<Result<(), SegmenterError>>,
    ) {
        let (tx, rx) = mpsc::channel(self.options.channel_size);
        let handle = tokio::spawn(self.do_loop(tx, ct));
        (ReceiverStream::new(rx), handle)
    }

    async fn do_loop(
        mut self,
        tx: mpsc::Sender<SnapshotChange>,
        ct: CancellationToken,
    ) -> Result<(), SegmenterError> {
        info!("starting segmenter");

        let mut ingestion_stream = self
            .ingestion_stream
            .take()
            .ok_or(SegmenterError::Initialization)?;

        let initialize = ingestion_stream
            .next()
            .await
            .ok_or(SegmenterError::Initialization)
            .attach_printable("ingestion stream ended unexpectedly")?;

        let (head, finalized) = match initialize {
            BlockIngestionEvent::Initialize { head, finalized } => (head, finalized),
            _ => {
                return Err(SegmenterError::Initialization)
                    .attach_printable("expected initialize message")
            }
        };

        debug!(%finalized, %head, "segmenter initialized");

        self.head = head;
        self.finalized = finalized;

        let starting_snapshot = self
            .snapshot_manager
            .read()
            .await
            .change_context(SegmenterError::Storage)?;

        if let Some(snapshot) = &starting_snapshot {
            self.snapshot = snapshot.clone();
        } else {
            self.snapshot = Snapshot::with_options(self.segment_options.clone());
        }

        // Reset the extra segment count since it's always starting from the start of the group.
        self.snapshot.ingestion.extra_segment_count = 0;

        let permit = tx
            .reserve()
            .await
            .change_context(SegmenterError::StreamClosed)?;
        permit.send(SnapshotChange::Started {
            snapshot: self.snapshot.clone(),
        });

        loop {
            tokio::select! {
                biased;
                _ = ct.cancelled() => break,

                // Only continue if we have capacity to send.
                Some(event) = ingestion_stream.next(), if tx.capacity() > 0 => {
                    match event {
                        BlockIngestionEvent::NewHead(head) => {
                            self.head = head;
                            self.write_segment_if_needed().await?;
                        },
                        BlockIngestionEvent::NewFinalized(finalized) => {
                            self.finalized = finalized;
                            self.write_segment_if_needed().await?;
                        },
                        BlockIngestionEvent::Ingested { cursor, prefix, filename } => {
                            self.forward_block_to_segment_builder(cursor, &prefix, &filename).await?;
                            self.write_segment_if_needed().await?;
                        },
                        BlockIngestionEvent::Invalidate { .. } => {
                            todo!();
                        },
                        BlockIngestionEvent::Initialize { .. } => {
                            return Err(SegmenterError::UnexpectedEvent)
                                .attach_printable("unexpected initialize message");
                        },
                    }
                }

                permit = tx.reserve(), if !self.message_queue.is_empty() => {
                    let permit = permit.change_context(SegmenterError::StreamClosed)?;
                    if let Some(message) = self.message_queue.pop_front() {
                        permit.send(message);
                    }
                }

                else => {
                    return Err(SegmenterError::UnexpectedEvent)
                        .attach_printable("ingestion stream ended unexpectedly");
                }
            }
        }

        Ok(())
    }

    async fn forward_block_to_segment_builder(
        &mut self,
        cursor: Cursor,
        prefix: &str,
        filename: &str,
    ) -> Result<(), SegmenterError> {
        {
            let bytes = self
                .local_storage
                .mmap(prefix, filename)
                .change_context(SegmenterError::Storage)?;

            let block = unsafe { rkyv::from_bytes_unchecked::<T>(&bytes) }
                .change_context(SegmenterError::Storage)?;

            self.segment_builder
                .add_block(block)
                .await
                .change_context(SegmenterError::Storage)
                .attach_printable("failed to add block to segment builder")
                .attach_printable_lazy(|| format!("cursor: {cursor}"))?;
        }

        self.current = cursor;
        self.block_count += 1;

        // Only notify if the block won't become part of a new segment any time soon.
        let finalized_segment_start = self.segment_options.segment_start(self.finalized.number);
        if finalized_segment_start <= self.current.number {
            info!(block = %self.current, "block ingested");
            self.message_queue.push_back(SnapshotChange::BlockIngested {
                cursor: self.current.clone(),
            })
        }

        /* TODO: figure out when it's safe to delete old blocks.
        self.local_storage
            .remove_prefix(prefix)
            .await
            .change_context(SegmenterError::Storage)?;
        */

        Ok(())
    }

    async fn write_segment_if_needed(&mut self) -> Result<(), SegmenterError> {
        while self.do_write_segment_if_needed().await? {
            let new_state = self.snapshot.ingestion.clone();

            self.message_queue.push_back(SnapshotChange::StateChanged {
                new_state,
                finalized: self.finalized.clone(),
            });
        }

        Ok(())
    }

    async fn do_write_segment_if_needed(&mut self) -> Result<bool, SegmenterError> {
        if self.block_count < self.segment_options.segment_size {
            debug!(block_count = self.block_count, "done: not enough blocks");
            return Ok(false);
        }

        if self.current.number > self.finalized.number {
            debug!(
                current = self.current.number,
                finalized = self.finalized.number,
                "done: data is not finalized yet"
            );
            return Ok(false);
        }

        // TODO: handle segments that were ingested when not finalized.
        assert!(self.block_count == self.segment_options.segment_size);

        let segment_data = self
            .segment_builder
            .take_segment()
            .await
            .change_context(SegmenterError::Storage)?;

        let segment_name = self
            .segment_options
            .format_segment_name(self.current.number);

        info!(segment_name = %segment_name, "writing segment data");
        for segment in segment_data {
            debug!(segment_name = %segment_name, filename = %segment.filename, "writing segment data");
            let mut writer = self
                .storage
                .put(segment_prefix(&segment_name), &segment.filename)
                .await
                .change_context(SegmenterError::Storage)?;
            writer
                .write_all(&segment.data)
                .await
                .change_context(SegmenterError::Storage)
                .attach_printable("failed to write segment data")
                .attach_printable_lazy(|| {
                    format!(
                        "segment name: {} filename: {}",
                        segment_name, segment.filename
                    )
                })?;
            writer
                .shutdown()
                .await
                .change_context(SegmenterError::Storage)?;
        }

        self.block_count = 0;
        self.segment_count += 1;

        self.snapshot.ingestion.extra_segment_count += 1;

        if self.segment_count < self.segment_options.group_size {
            debug!(
                segment_count = self.segment_count,
                "done: not enough segments"
            );

            self.snapshot.revision += 1;
            self.snapshot_manager
                .write(&self.snapshot)
                .await
                .change_context(SegmenterError::Storage)?;

            return Ok(true);
        }

        assert!(self.segment_count == self.segment_options.group_size);

        let segment_group_name = self
            .segment_options
            .format_segment_group_name(self.current.number);
        let segment_group_data = self
            .segment_builder
            .take_segment_group()
            .await
            .change_context(SegmenterError::Storage)?;

        info!(segment_group_name = %segment_group_name, "writing segment group data");

        let mut writer = self
            .storage
            .put("group", segment_group_name)
            .await
            .change_context(SegmenterError::Storage)?;

        writer
            .write_all(&segment_group_data.data)
            .await
            .change_context(SegmenterError::Storage)?;

        writer
            .shutdown()
            .await
            .change_context(SegmenterError::Storage)?;

        self.segment_count = 0;

        self.snapshot.revision += 1;
        self.snapshot.ingestion.group_count += 1;
        self.snapshot.ingestion.extra_segment_count = 0;

        self.snapshot_manager
            .write(&self.snapshot)
            .await
            .change_context(SegmenterError::Storage)?;

        Ok(true)
    }
}

impl error_stack::Context for SegmenterError {}

impl std::fmt::Display for SegmenterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmenterError::Initialization => write!(f, "Segmenter initialization error"),
            SegmenterError::UnexpectedEvent => write!(f, "Unexpected block event"),
            SegmenterError::Storage => write!(f, "Error writing segment to storage"),
            SegmenterError::Snapshot => write!(f, "Snapshot error"),
            SegmenterError::StreamClosed => write!(f, "Output stream closed"),
            SegmenterError::InvalidState => write!(f, "Segmenter is in an invalid state"),
        }
    }
}

impl Default for SegmenterOptions {
    fn default() -> Self {
        Self { channel_size: 1024 }
    }
}
