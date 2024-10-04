use apibara_etcd::{EtcdClient, Lock};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    block_store::{BlockStoreReader, BlockStoreWriter},
    chain_view::ChainView,
    compaction::group::SegmentGroupService,
    file_cache::FileCache,
    ingestion::IngestionStateClient,
    object_store::ObjectStore,
};

use super::{error::CompactionError, segment::SegmentService};

#[derive(Debug, Clone)]
pub struct CompactionServiceOptions {
    /// How many blocks in a single segment.
    pub segment_size: usize,
    /// How many segments in a single segment group.
    pub group_size: usize,
}

pub struct CompactionService {
    options: CompactionServiceOptions,
    block_store_reader: BlockStoreReader,
    block_store_writer: BlockStoreWriter,
    state_client: IngestionStateClient,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
}

impl CompactionService {
    pub fn new(
        etcd_client: EtcdClient,
        object_store: ObjectStore,
        file_cache: FileCache,
        chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
        options: CompactionServiceOptions,
    ) -> Self {
        let block_store_reader = BlockStoreReader::new(object_store.clone(), file_cache);
        let block_store_writer = BlockStoreWriter::new(object_store);
        let state_client = IngestionStateClient::new(&etcd_client);

        Self {
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

        let segment_service = SegmentService::new(
            self.options.segment_size,
            chain_view.clone(),
            self.block_store_reader.clone(),
            self.block_store_writer.clone(),
            self.state_client.clone(),
        );

        let group_service = SegmentGroupService::new(
            self.options.segment_size,
            self.options.group_size,
            chain_view.clone(),
            self.block_store_reader.clone(),
            self.block_store_writer.clone(),
            self.state_client.clone(),
        );

        let segment_service_handle = tokio::spawn(segment_service.start(ct.clone()));
        let group_service_handle = tokio::spawn(group_service.start(ct.clone()));

        let lock_handle = lock_keep_alive_loop(lock, ct.clone());

        tokio::select! {
            _ = ct.cancelled() => {
                Ok(())
            }
            lock = lock_handle => {
                info!("compaction lock loop terminated");
                lock.change_context(CompactionError)
            }
            segment_service = segment_service_handle => {
                info!("compaction segmentation loop terminated");
                segment_service.change_context(CompactionError)?.change_context(CompactionError)
            }
            group_service = group_service_handle => {
                info!("compaction group service loop terminated");
                group_service.change_context(CompactionError)?.change_context(CompactionError)
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
            segment_size: 1_000,
            group_size: 100,
        }
    }
}
