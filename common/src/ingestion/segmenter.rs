use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use futures_util::{Stream, StreamExt, TryFutureExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{core::Cursor, ingestion::IngestedBlock, storage::StorageBackend};

use super::{BlockEvent, Snapshot, SnapshotChange, SnapshotManager};

#[async_trait]
pub trait SegmentBuilder {
    type Error: error_stack::Context;

    fn create_segment(&mut self, cursors: &[Cursor]) -> Result<(), Self::Error>;

    async fn write_segment<S>(
        &mut self,
        segment_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error>
    where
        S: StorageBackend + Send,
        <S as StorageBackend>::Writer: Send;

    async fn write_segment_group<S>(
        &mut self,
        segment_group_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error>
    where
        S: StorageBackend + Send,
        <S as StorageBackend>::Writer: Send;

    async fn cleanup_segment_data(&mut self, cursors: &[Cursor]) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub enum SegmenterError {
    Initialization,
    UnexpectedEvent,
    Storage,
    Snapshot,
}

pub struct Segmenter<B, S, I>
where
    B: SegmentBuilder + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    segment_builder: B,
    storage: S,
    snapshot_manager: SnapshotManager<S>,
    ingestion_stream: I,
}

impl<B, S, I> Segmenter<B, S, I>
where
    B: SegmentBuilder + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin,
    <S as StorageBackend>::Writer: Send,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        segment_builder: B,
        storage: S,
        snapshot_manager: SnapshotManager<S>,
        ingestion_stream: I,
    ) -> Self {
        Self {
            segment_builder,
            storage,
            snapshot_manager,
            ingestion_stream,
        }
    }

    pub async fn start(
        self,
        starting_snapshot: Snapshot,
        ct: CancellationToken,
    ) -> Result<impl Stream<Item = SnapshotChange>, SegmenterError> {
        let (tx, rx) = mpsc::channel(1024);
        let segmenter_loop = SegmenterLoop::initialize(
            self.segment_builder,
            self.storage,
            self.snapshot_manager,
            self.ingestion_stream,
            tx,
            starting_snapshot,
        )
        .await?;

        tokio::spawn(segmenter_loop.do_loop(ct).inspect_err(|err| {
            error!(error = ?err, "segmenter loop error");
        }));

        Ok(ReceiverStream::new(rx))
    }
}

pub struct SegmenterLoop<B, S, I>
where
    B: SegmentBuilder + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    segment_builder: B,
    storage: S,
    snapshot_manager: SnapshotManager<S>,
    ingestion_stream: I,
    tx: mpsc::Sender<SnapshotChange>,
    snapshot: Snapshot,
    finalized: Cursor,
    segment: Option<SegmentData>,
}

struct SegmentData {
    group_start: Cursor,
    cursors: Vec<Cursor>,
}

impl<B, S, I> SegmenterLoop<B, S, I>
where
    B: SegmentBuilder + Send + Sync + 'static,
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin,
    <S as StorageBackend>::Writer: Send,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    pub async fn initialize(
        segment_builder: B,
        storage: S,
        snapshot_manager: SnapshotManager<S>,
        mut ingestion_stream: I,
        tx: mpsc::Sender<SnapshotChange>,
        snapshot: Snapshot,
    ) -> Result<Self, SegmenterError> {
        let finalized = {
            let Some(event) = ingestion_stream.next().await else {
                return Err(SegmenterError::Initialization)
                    .attach_printable("ingestion events stream ended unexpectedly");
            };

            let BlockEvent::Started { finalized } = event else {
                return Err(SegmenterError::Initialization)
                    .attach_printable("expected first event to be BlockEvent::Started");
            };

            finalized
        };

        let Ok(_) = tx.send(SnapshotChange::Started(snapshot.clone())).await else {
            todo!();
        };

        Ok(SegmenterLoop {
            segment_builder,
            storage,
            snapshot_manager,
            ingestion_stream,
            tx,
            snapshot,
            finalized,
            segment: None,
        })
    }

    pub async fn do_loop(mut self, ct: CancellationToken) -> Result<(), SegmenterError> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => break,
                event = self.ingestion_stream.next() => {
                    let Some(event) = event else {
                        return Err(SegmenterError::Initialization)
                            .attach_printable("ingestion events stream ended unexpectedly");
                    };

                    self.handle_event(event).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_event(&mut self, event: BlockEvent) -> Result<(), SegmenterError> {
        match event {
            BlockEvent::Ingested(cursor) => {
                debug!(cursor = %cursor, "new block ingested");

                // Check if we need to start a new segment group.
                let Some(segment_data) = self.segment.as_mut() else {
                    self.segment = Some(SegmentData {
                        group_start: cursor.clone(),
                        cursors: vec![cursor.clone()],
                    });

                    let Ok(_) = self
                        .tx
                        .send(SnapshotChange::BlockIngested(IngestedBlock { cursor }))
                        .await
                    else {
                        todo!();
                    };

                    return Ok(());
                };

                // Add to existing segment.
                segment_data.cursors.push(cursor.clone());

                let finalized_segment_start = self
                    .snapshot
                    .segment_options
                    .segment_start(self.finalized.number);

                // Since the segment can only contains finalized data, if the current cursor is in
                // the same segment as the finalized cursor, we can emit an event and return early.
                if cursor.number >= finalized_segment_start {
                    let Ok(_) = self
                        .tx
                        .send(SnapshotChange::BlockIngested(IngestedBlock { cursor }))
                        .await
                    else {
                        todo!();
                    };

                    return Ok(());
                }

                self.write_segment_if_needed().await
            }
            BlockEvent::Finalized(cursor) => {
                info!(cursor = %cursor, "finalized cursor updated");
                self.finalized = cursor;
                self.write_segment_if_needed().await
            }
            BlockEvent::Invalidate => {
                todo!();
            }
            _ => Err(SegmenterError::UnexpectedEvent)
                .attach_printable("unexpected event in segmenter loop")
                .attach_printable_lazy(|| format!("event: {:?}", event)),
        }
    }

    /// Write segment and segment groups if needed.
    async fn write_segment_if_needed(&mut self) -> Result<(), SegmenterError> {
        while self.do_write_segment_if_needed().await? {
            let new_state = self.snapshot.ingestion.clone();
            let Ok(_) = self
                .tx
                .send(SnapshotChange::StateChanged {
                    new_state,
                    finalized: self.finalized.clone(),
                })
                .await
            else {
                todo!();
            };
        }

        Ok(())
    }

    /// Actually write segment if needed. Returns `true` if it wrote a segment.
    async fn do_write_segment_if_needed(&mut self) -> Result<bool, SegmenterError> {
        let Some(segment_data) = self.segment.as_mut() else {
            debug!("done: no segment data");
            return Ok(false);
        };

        if segment_data.cursors.is_empty() {
            debug!("done: no cursors");
            return Ok(false);
        }

        let segment_options = &self.snapshot.segment_options;

        let Some(first_cursor) = segment_data.cursors.first() else {
            debug!(
                cursors_len = segment_data.cursors.len(),
                "done: no first cursor"
            );
            return Ok(false);
        };

        let Some(last_cursor) = segment_data.cursors.get(segment_options.segment_size - 1) else {
            debug!(
                cursors_len = segment_data.cursors.len(),
                "done: not enough cursors"
            );
            return Ok(false);
        };

        // Data is not finalized yet.
        if last_cursor.number > self.finalized.number {
            debug!(
                last = last_cursor.number,
                finalized = self.finalized.number,
                "done: data is not finalized yet"
            );
            return Ok(false);
        }

        // We may have more cursors than needed, so we just take as many as we need.
        // This happens when we reach the tip of the chain and we're just waiting for
        // the finalized cursor to move.
        let current_segment_start = segment_options.segment_start(first_cursor.number);
        let next_segment_start = segment_options.segment_start(last_cursor.number + 1);
        let cursors_to_segment = {
            let cursors = std::mem::take(&mut segment_data.cursors);
            let mut cursors_to_segment = Vec::new();
            let mut cursors_to_keep = Vec::new();
            for cursor in cursors {
                if cursor.number >= next_segment_start {
                    cursors_to_keep.push(cursor);
                } else {
                    cursors_to_segment.push(cursor);
                }
            }

            segment_data.cursors = cursors_to_keep;
            cursors_to_segment
        };

        let current_group_start =
            segment_options.segment_group_start(segment_data.group_start.number);

        let next_group_start = segment_options.segment_group_start(
            cursors_to_segment
                .last()
                .expect("at least one cursor")
                .number
                + 1,
        );

        debug!("copying blocks to segment");
        self.segment_builder
            .create_segment(&cursors_to_segment)
            .change_context(SegmenterError::Storage)
            .attach_printable("failed to create segment")?;

        let segment_name = segment_options.format_segment_name(current_segment_start);
        self.segment_builder
            .write_segment(&segment_name, &mut self.storage)
            .await
            .change_context(SegmenterError::Storage)
            .attach_printable("failed to write segment")
            .attach_printable_lazy(|| format!("segment_name: {segment_name}"))?;
        info!(segment_name, "segment written");

        debug!("delete old block data");
        self.segment_builder
            .cleanup_segment_data(&cursors_to_segment)
            .await
            .change_context(SegmenterError::Storage)
            .attach_printable("failed to cleanup segment data")?;

        self.snapshot.revision += 1;
        self.snapshot.ingestion.extra_segment_count += 1;

        // No need to also update segment group.
        // Just update snapshot.
        if current_group_start == next_group_start {
            self.write_snapshot().await?;

            return Ok(true);
        }

        let group_name = segment_options.format_segment_group_name(current_group_start);
        self.segment_builder
            .write_segment_group(&group_name, &mut self.storage)
            .await
            .change_context(SegmenterError::Storage)
            .attach_printable("failed to write segment group")
            .attach_printable_lazy(|| format!("group_name: {group_name}"))?;
        info!(group_name, "segment group written");

        self.snapshot.ingestion.group_count += 1;
        self.snapshot.ingestion.extra_segment_count = 0;

        // If there are still cursors to process, start a new segment group.
        if let Some(first_cursor) = segment_data.cursors.first() {
            segment_data.group_start = first_cursor.clone();
        } else {
            self.segment = None;
        }

        self.write_snapshot().await?;

        Ok(true)
    }

    async fn write_snapshot(&mut self) -> Result<(), SegmenterError> {
        self.snapshot_manager
            .write(&self.snapshot)
            .await
            .change_context(SegmenterError::Snapshot)
            .attach_printable("failed to write snapshot")
    }
}

impl error_stack::Context for SegmenterError {}

impl std::fmt::Display for SegmenterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmenterError::Initialization => write!(f, "segmenter initialization error"),
            SegmenterError::UnexpectedEvent => write!(f, "unexpected block event"),
            SegmenterError::Storage => write!(f, "error writing segment to storage"),
            SegmenterError::Snapshot => write!(f, "snapshot error"),
        }
    }
}
