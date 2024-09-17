use apibara_etcd::{EtcdClient, Lock};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    block_store::{BlockStoreReader, BlockStoreWriter},
    chain_view::ChainView,
    file_cache::{FileCache, Mmap},
    ingestion::IngestionStateClient,
    object_store::ObjectStore,
    store::segment::SerializedSegment,
    Cursor,
};

use super::{error::CompactionError, segment::SegmentService};

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
    B: SegmentBuilder + Send + Sync + 'static,
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
        self,
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

        let segment_service = SegmentService {
            builder: self.builder,
            segment_size: self.options.segment_size,
            chain_view,
            block_store_reader: self.block_store_reader.clone(),
            block_store_writer: self.block_store_writer.clone(),
            state_client: self.state_client.clone(),
        };

        let segment_service_handle = tokio::spawn(segment_service.start(ct.clone()));

        let lock_handle = lock_keep_alive_loop(lock, ct.clone());

        tokio::select! {
            _ = ct.cancelled() => {
                Ok(())
            }
            _ = lock_handle => {
                Ok(())
            }
            _ = segment_service_handle => {
                Ok(())
            }
        }
    }
}

async fn lock_keep_alive_loop(
    lock: &mut Lock,
    ct: CancellationToken,
) -> Result<(), CompactionError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                return Ok(());
            }
            _ = interval.tick() => {
                debug!("compaction: keep alive");
                lock.keep_alive().await.change_context(CompactionError)?;
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
