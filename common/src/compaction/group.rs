use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

                for i in 0..self.group_size {
                    let segment_start =
                        first_block_in_group.number + (i * self.segment_size) as u64;
                    let current_cursor = Cursor::new_finalized(segment_start);
                    let segment = self
                        .block_store_reader
                        .get_index_segment(&current_cursor)
                        .await
                        .change_context(CompactionError)?;

                    let segment = rkyv::from_bytes::<
                        Segment<IndexGroupFragment>,
                        rkyv::rancor::Error,
                    >(&segment)
                    .change_context(CompactionError)?;

                    builder
                        .add_segment(&segment)
                        .change_context(CompactionError)
                        .attach_printable("failed to add segment to group")?;
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
