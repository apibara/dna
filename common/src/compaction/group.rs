use error_stack::{Result, ResultExt};
use futures_buffered::FuturesOrderedBounded;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    block_store::{BlockStoreWriter, UncachedBlockStoreReader},
    chain_view::{ChainView, NextCursor},
    compaction::group_builder::SegmentGroupBuilder,
    fragment::IndexGroupFragment,
    ingestion::IngestionStateClient,
    segment::Segment,
    Cursor,
};

use super::CompactionError;

const MAX_BUFFERED_SEGMENTS: usize = 128;

pub struct SegmentGroupService {
    segment_size: usize,
    group_size: usize,
    chain_view: ChainView,
    block_store_reader: UncachedBlockStoreReader,
    block_store_writer: BlockStoreWriter,
    state_client: IngestionStateClient,
}

impl SegmentGroupService {
    pub fn new(
        segment_size: usize,
        group_size: usize,
        chain_view: ChainView,
        block_store_reader: UncachedBlockStoreReader,
        block_store_writer: BlockStoreWriter,
        state_client: IngestionStateClient,
    ) -> Self {
        Self {
            segment_size,
            group_size,
            chain_view,
            block_store_reader,
            block_store_writer,
            state_client,
        }
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), CompactionError> {
        let chain_view = self.chain_view;

        let blocks_in_group = (self.group_size * self.segment_size) as u64;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let first_block_in_group = if let Some(cursor) = chain_view
                .get_grouped_cursor()
                .await
                .change_context(CompactionError)?
            {
                let NextCursor::Continue { cursor, .. } = chain_view
                    .get_next_cursor(&Some(cursor.clone()))
                    .await
                    .change_context(CompactionError)?
                else {
                    return Err(CompactionError)
                        .attach_printable("chain view returned invalid cursor")
                        .attach_printable_lazy(|| format!("cursor: {cursor}"));
                };
                cursor
            } else {
                chain_view
                    .get_starting_cursor()
                    .await
                    .change_context(CompactionError)?
            };

            let Some(segmented) = chain_view
                .get_segmented_cursor()
                .await
                .change_context(CompactionError)?
            else {
                let Some(_) = ct.run_until_cancelled(chain_view.segmented_changed()).await else {
                    return Ok(());
                };
                continue;
            };

            info!(
                next_cursor = %first_block_in_group,
                blocks_in_group = %blocks_in_group,
                segmented = %segmented,
                "compaction: group tick"
            );

            if first_block_in_group.number + blocks_in_group <= segmented.number {
                info!(starting_cursor = %first_block_in_group, "creating new group");

                let mut builder = SegmentGroupBuilder::new(self.segment_size);

                let buffered_queue_size = usize::min(self.group_size, MAX_BUFFERED_SEGMENTS);
                let mut segment_queue = FuturesOrderedBounded::new(buffered_queue_size);

                for i in 0..buffered_queue_size {
                    let segment_start =
                        first_block_in_group.number + (i * self.segment_size) as u64;
                    let current_cursor = Cursor::new_finalized(segment_start);

                    segment_queue.push_back(
                        self.block_store_reader
                            .get_index_segment_and_cursor(current_cursor),
                    );

                    debug!(segment_start, "group compaction: pushed segment to queue");
                }

                if self.group_size > buffered_queue_size {
                    debug!("compaction: queue full, waiting for segments");

                    for i in buffered_queue_size..self.group_size {
                        let (segment_cursor, segment_data) = segment_queue
                            .next()
                            .await
                            .ok_or(CompactionError)
                            .attach_printable("compaction group buffer is empty")?
                            .change_context(CompactionError)
                            .attach_printable("failed to get segment")?;

                        let segment = rkyv::from_bytes::<
                            Segment<IndexGroupFragment>,
                            rkyv::rancor::Error,
                        >(&segment_data)
                        .change_context(CompactionError)?;

                        builder
                            .add_segment(&segment)
                            .change_context(CompactionError)
                            .attach_printable("failed to add segment to group")?;

                        debug!(cursor = %segment_cursor, "group compaction: added segment to group");

                        let segment_start =
                            first_block_in_group.number + (i * self.segment_size) as u64;
                        let current_cursor = Cursor::new_finalized(segment_start);

                        segment_queue.push_back(
                            self.block_store_reader
                                .get_index_segment_and_cursor(current_cursor),
                        );

                        debug!(segment_start, "group compaction: pushed segment to queue");
                    }
                }

                debug!("compaction: pushed all segments to queue");

                for _ in 0..buffered_queue_size {
                    let (segment_cursor, segment_data) = segment_queue
                        .next()
                        .await
                        .ok_or(CompactionError)
                        .attach_printable("compaction group buffer is empty")?
                        .change_context(CompactionError)
                        .attach_printable("failed to get segment")?;

                    let segment = rkyv::from_bytes::<
                        Segment<IndexGroupFragment>,
                        rkyv::rancor::Error,
                    >(&segment_data)
                    .change_context(CompactionError)?;

                    builder
                        .add_segment(&segment)
                        .change_context(CompactionError)
                        .attach_printable("failed to add segment to group")?;

                    debug!(cursor = %segment_cursor, "group compaction: added segment to group");
                }

                // Sanity checks
                if builder.segment_count != self.group_size {
                    return Err(CompactionError)
                        .attach_printable("builder segment count does not match group size")
                        .attach_printable_lazy(|| {
                            format!(
                                "builder: {:}, group: {:}",
                                builder.segment_count, self.group_size
                            )
                        });
                }

                let group = builder.build().change_context(CompactionError)?;
                let last_block_in_group = first_block_in_group.number + blocks_in_group - 1;

                info!(
                    first_block = %first_block_in_group,
                    last_block = %last_block_in_group,
                    "uploading group to object store"
                );

                self.block_store_writer
                    .put_group(&first_block_in_group, &group)
                    .await
                    .change_context(CompactionError)?;

                self.state_client
                    .put_grouped(last_block_in_group)
                    .await
                    .change_context(CompactionError)?;
            } else {
                info!("compaction waiting for segmented change");
                let Some(_) = ct.run_until_cancelled(chain_view.segmented_changed()).await else {
                    return Ok(());
                };
            }
        }
    }
}
