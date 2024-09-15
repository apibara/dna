use apibara_etcd::{EtcdClient, Lock};
use error_stack::{Result, ResultExt};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_store::{BlockStoreReader, BlockStoreWriter},
    chain_view::{ChainView, NextCursor},
    file_cache::{FileCache, Mmap},
    ingestion::IngestionStateClient,
    object_store::ObjectStore,
    store::segment::SerializedSegment,
    Cursor,
};

use super::error::CompactionError;

pub trait SegmentBuilder: Clone {
    fn start_new_segment(&mut self, first_block: Cursor) -> Result<(), CompactionError>;
    fn add_block(&mut self, cursor: &Cursor, bytes: Mmap) -> Result<(), CompactionError>;
    fn segment_data(&mut self) -> Result<Vec<SerializedSegment>, CompactionError>;
}

#[derive(Debug, Clone)]
pub struct CompactionServiceOptions {
    /// How many blocks in a single segment.
    pub segment_size: usize,
}

pub struct CompactionService<B>
where
    B: SegmentBuilder,
{
    builder: B,
    options: CompactionServiceOptions,
    block_store_reader: BlockStoreReader,
    block_store_writer: BlockStoreWriter,
    state_client: IngestionStateClient,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
}

impl<B> CompactionService<B>
where
    B: SegmentBuilder,
{
    pub fn new(
        builder: B,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
        chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
        options: CompactionServiceOptions,
    ) -> Self {
        let file_cache = FileCache::disabled();
        let block_store_reader = BlockStoreReader::new(object_store.clone(), file_cache.clone());
        let block_store_writer = BlockStoreWriter::new(object_store);
        let state_client = IngestionStateClient::new(&etcd_client);

        Self {
            builder,
            options,
            block_store_reader,
            block_store_writer,
            chain_view,
            state_client,
        }
    }

    pub async fn start(
        mut self,
        lock: &mut Lock,
        ct: CancellationToken,
    ) -> Result<(), CompactionError> {
        let chain_view = loop {
            lock.keep_alive().await.change_context(CompactionError)?;

            if let Some(chain_view) = self.chain_view.borrow().clone() {
                break chain_view;
            };

            let Some(_) = ct
                .run_until_cancelled(tokio::time::sleep(std::time::Duration::from_secs(5)))
                .await
            else {
                return Ok(());
            };
        };

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            lock.keep_alive().await.change_context(CompactionError)?;

            let first_block_in_segment = if let Some(cursor) = chain_view
                .get_segmented_cursor()
                .await
                .change_context(CompactionError)?
            {
                let NextCursor::Continue(cursor) = chain_view
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

            let head = chain_view
                .get_head()
                .await
                .change_context(CompactionError)?;
            let finalized = chain_view
                .get_finalized_cursor()
                .await
                .change_context(CompactionError)?;

            info!(
                next_cursor = %first_block_in_segment,
                head = %head,
                finalized = %finalized,
                "compaction: tick"
            );

            let latest_available = u64::min(finalized.number, head.number);

            if first_block_in_segment.number + self.options.segment_size as u64 <= latest_available
            {
                info!(
                    starting_cursor = %first_block_in_segment,
                    "creating new segment"
                );

                let mut current = first_block_in_segment.clone();
                let mut last_block_in_segment = first_block_in_segment.clone();

                self.builder
                    .start_new_segment(current.clone())
                    .change_context(CompactionError)?;

                for _ in 0..self.options.segment_size {
                    lock.keep_alive().await.change_context(CompactionError)?;

                    let bytes = self
                        .block_store_reader
                        .get_block(&current)
                        .await
                        .change_context(CompactionError)
                        .attach_printable("failed to get block")
                        .attach_printable_lazy(|| format!("cursor: {current}"))?;

                    self.builder
                        .add_block(&current, bytes)
                        .change_context(CompactionError)
                        .attach_printable("failed to add block to segment")
                        .attach_printable_lazy(|| format!("cursor: {current}"))?;

                    let NextCursor::Continue(next_cursor) = chain_view
                        .get_next_cursor(&Some(current.clone()))
                        .await
                        .change_context(CompactionError)?
                    else {
                        return Err(CompactionError)
                            .attach_printable("chain view returned invalid next cursor")
                            .attach_printable_lazy(|| format!("cursor: {current}"));
                    };

                    last_block_in_segment = current.clone();
                    current = next_cursor;
                }

                let segment_data = self
                    .builder
                    .segment_data()
                    .change_context(CompactionError)?;

                info!(
                    first_block = %first_block_in_segment,
                    last_block = %last_block_in_segment,
                     "uploading segment to object store"
                );

                for segment in segment_data {
                    self.block_store_writer
                        .put_segment(&first_block_in_segment, &segment)
                        .await
                        .change_context(CompactionError)
                        .attach_printable("failed to put segment")?;
                }

                self.state_client
                    .put_segmented(last_block_in_segment.number)
                    .await
                    .change_context(CompactionError)
                    .attach_printable("failed to put segmented block")?;
            } else {
                let state_change = if finalized.number < head.number {
                    info!("compaction waiting for finalized change");
                    chain_view.finalized_changed().boxed()
                } else {
                    info!("compaction waiting for head change");
                    chain_view.head_changed().boxed()
                };

                tokio::pin!(state_change);

                let mut keep_alive_interval =
                    tokio::time::interval(std::time::Duration::from_secs(5));

                loop {
                    tokio::select! {
                        _ = ct.cancelled() => return Ok(()),
                        _ = keep_alive_interval.tick() => {
                            lock.keep_alive().await.change_context(CompactionError)?;
                        }
                        _ = &mut state_change => {
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl Default for CompactionServiceOptions {
    fn default() -> Self {
        Self {
            segment_size: 10_000,
        }
    }
}
