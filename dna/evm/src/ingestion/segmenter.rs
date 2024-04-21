use std::time::Duration;

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    ingestion::{IngestedBlock, IngestionState, SealGroup, Segment, Snapshot, SnapshotChange},
    segment::SegmentOptions,
    server::SnapshotState,
    storage::{LocalStorageBackend, StorageBackend},
};
use error_stack::ResultExt;
use futures_util::{Stream, TryFutureExt};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use super::downloader::BlockEvent;

pub struct SegmenterService<
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
> {
    local_storage: LocalStorageBackend,
    storage: S,
    ingestion_events: I,
}

struct Worker<S: StorageBackend + Send + Sync + 'static> {
    local_storage: LocalStorageBackend,
    storage: S,
    tx: mpsc::Sender<SnapshotChange>,
    snapshot: Snapshot,
    finalized: Cursor,
    segment: Option<SegmentData>,
}

struct SegmentData {
    group_start: Cursor,
    cursors: Vec<Cursor>,
}

impl<S, I> SegmenterService<S, I>
where
    S: StorageBackend + Send + Sync + 'static,
    I: Stream<Item = BlockEvent> + Unpin + Send + Sync + 'static,
{
    pub fn new(local_storage: LocalStorageBackend, storage: S, ingestion_events: I) -> Self {
        Self {
            local_storage,
            storage,
            ingestion_events,
        }
    }

    pub fn start(
        self,
        starting_snapshot: Snapshot,
        ct: CancellationToken,
    ) -> impl Stream<Item = SnapshotChange> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(
            self.segmenter_loop(starting_snapshot, tx, ct)
                .inspect_err(|err| {
                    error!(err = ?err, "segmenter loop returned with error");
                }),
        );

        ReceiverStream::new(rx)
    }

    async fn segmenter_loop(
        self,
        starting_snapshot: Snapshot,
        tx: mpsc::Sender<SnapshotChange>,
        ct: CancellationToken,
    ) -> Result<()> {
        let mut ingestion_events = self.ingestion_events;

        // Track finalized cursor since segments can only contain finalized data.
        let finalized = {
            let Some(event) = ingestion_events.next().await else {
                return Err(DnaError::Fatal)
                    .attach_printable("ingestion events stream ended unexpectedly");
            };

            match event {
                BlockEvent::Started { finalized } => finalized,
                _ => {
                    return Err(DnaError::Fatal)
                        .attach_printable("expected first event to be BlockEvent::Started")
                }
            }
        };

        let Ok(_) = tx
            .send(SnapshotChange::Started(starting_snapshot.clone()))
            .await
        else {
            return Ok(());
        };

        let mut worker = Worker {
            local_storage: self.local_storage,
            storage: self.storage,
            tx,
            snapshot: starting_snapshot,
            finalized,
            segment: None,
        };

        loop {
            tokio::select! {
                _ = ct.cancelled() => break,
                event = ingestion_events.next() => {
                    let Some(event) = event else {
                        return Err(DnaError::Fatal)
                            .attach_printable("ingestion events stream ended unexpectedly");
                    };

                    worker.handle_event(event).await
                        .attach_printable("failed to handle ingestion event")?;
                }
            }
        }

        Ok(())
    }
}

impl<S> Worker<S>
where
    S: StorageBackend + Send + Sync + 'static,
{
    async fn handle_event(&mut self, event: BlockEvent) -> Result<()> {
        match event {
            BlockEvent::Ingested(cursor) => {
                debug!(cursor = ?cursor, "new block ingested");

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
                info!(
                    number = cursor.number,
                    hash = cursor.hash_as_hex(),
                    "finalized cursor updated"
                );
                self.finalized = cursor;
                self.write_segment_if_needed().await
            }
            BlockEvent::Invalidate => {
                todo!();
            }
            _ => Err(DnaError::Fatal)
                .attach_printable("unexpected event in ingestion stream")
                .attach_printable_lazy(|| format!("event: {:?}", event)),
        }
    }

    /// Write segment and segment groups if needed.
    async fn write_segment_if_needed(&mut self) -> Result<()> {
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
    async fn do_write_segment_if_needed(&mut self) -> Result<bool> {
        // If segment is full and cursor is finalized:
        // - Read blocks and build segment
        // - Write segment to storage
        // - Delete blocks from local storage
        // - Update group
        // - maybe write group
        // - write snapshot
        // Always emit relevant events.
        let Some(segment_data) = self.segment.as_mut() else {
            return Ok(false);
        };

        if segment_data.cursors.is_empty() {
            return Ok(false);
        }

        let segment_options = &self.snapshot.segment_options;

        let Some(_first_cursor) = segment_data.cursors.first() else {
            return Ok(false);
        };

        let Some(last_cursor) = segment_data
            .cursors
            .iter()
            .nth(segment_options.segment_size - 1)
        else {
            return Ok(false);
        };

        // Data is not finalized yet.
        if last_cursor.number > self.finalized.number {
            debug!(
                last = last_cursor.number,
                finalized = self.finalized.number,
                "data is not finalized yet"
            );
            return Ok(false);
        }

        let next_segment_start = segment_options.segment_start(last_cursor.number + 1);

        info!("writing segment");
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

        // TODO: write blocks to segment.
        for cursor in cursors_to_segment {
            debug!(cursor = ?cursor, "writing block to segment");
        }

        self.snapshot.revision += 1;
        self.snapshot.ingestion.extra_segment_count += 1;

        if current_group_start == next_group_start {
            // TODO: update snapshot on disk.
            return Ok(true);
        }

        info!(segment_group = current_group_start, "writing segment group");

        // TODO: write segment group to storage.
        self.snapshot.ingestion.group_count += 1;
        self.snapshot.ingestion.extra_segment_count = 0;

        // TOO: update snapshot on disk.

        self.segment = None;
        Ok(true)
    }
}
