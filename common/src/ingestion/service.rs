use std::{future::Future, sync::Arc};

use error_stack::{Result, ResultExt};
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::{
    block_store::BlockStore,
    chain::{BlockInfo, CanonicalChainBuilder},
    chain_store::ChainStore,
    etcd::EtcdClient,
    object_store::ObjectStore,
    rkyv::Serializable,
    Cursor, Hash,
};

use super::{error::IngestionError, state_client::IngestionStateClient};

pub trait BlockIngestion {
    type Block;

    fn get_head_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;
    fn get_finalized_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;

    fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<(BlockInfo, Self::Block), IngestionError>> + Send;

    fn ingest_block_by_hash(
        &self,
        block_hash: impl Into<Hash> + Send,
    ) -> impl Future<Output = Result<(BlockInfo, Self::Block), IngestionError>> + Send;
}

type IngestionTaskHandle = JoinHandle<Result<BlockInfo, IngestionError>>;

#[derive(Debug)]
pub struct IngestionServiceOptions {
    /// Maximum number of concurrent ingestion tasks.
    pub max_concurrent_tasks: usize,
    /// How many blocks in a single chain segment.
    pub chain_segment_size: usize,
    /// How many finalized blocks to wait before uploading a chain segment.
    pub chain_segment_upload_offset_size: usize,
    /// Override the ingestion starting block.
    pub override_starting_block: Option<u64>,
}

pub struct IngestionService<B, I>
where
    I: BlockIngestion<Block = B>,
    B: Send + Sync + 'static,
{
    options: IngestionServiceOptions,
    ingestion: Arc<I>,
    state_client: IngestionStateClient,
    chain_store: ChainStore,
    block_store: BlockStore<B>,
    chain_builder: CanonicalChainBuilder,
    task_queue: FuturesOrdered<IngestionTaskHandle>,
}

enum IngestionState {
    IngestFinalized(IngestFinalizedState),
}

struct IngestFinalizedState {
    cursor: Cursor,
    finalized: Cursor,
    head: Cursor,
}

impl<B, I> IngestionService<B, I>
where
    I: BlockIngestion<Block = B> + Send + Sync + 'static,
    B: Send + Sync + 'static + for<'a> Serializable<'a>,
{
    pub fn new(
        ingestion: I,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
        options: IngestionServiceOptions,
    ) -> Self {
        let chain_store = ChainStore::new(object_store.clone());
        let block_store = BlockStore::new(object_store);
        let state_client = IngestionStateClient::new(&etcd_client);
        Self {
            options,
            ingestion: ingestion.into(),
            state_client,
            chain_store,
            block_store,
            chain_builder: CanonicalChainBuilder::new(),
            task_queue: FuturesOrdered::new(),
        }
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), IngestionError> {
        let head = self.ingestion.get_head_cursor().await?;
        let finalized = self.ingestion.get_finalized_cursor().await?;

        let starting_cursor = self.get_starting_cursor().await?;

        let mut state = if starting_cursor.number <= finalized.number {
            IngestionState::IngestFinalized(IngestFinalizedState {
                cursor: starting_cursor,
                finalized,
                head,
            })
        } else {
            todo!();
        };

        loop {
            if ct.is_cancelled() {
                break;
            }

            match state {
                IngestionState::IngestFinalized(finalized_state) => {
                    state = self.tick_finalized(finalized_state, ct.clone()).await?;
                }
            }
        }

        Ok(())
    }

    async fn tick_finalized(
        &mut self,
        state: IngestFinalizedState,
        ct: CancellationToken,
    ) -> Result<IngestionState, IngestionError> {
        let mut block_number = state.cursor.number;

        loop {
            tokio::select! {
                _ = ct.cancelled() => break,
                join_result = self.task_queue.next() => {
                    if let Some(join_result) = join_result {
                        let block_info = join_result
                            .change_context(IngestionError::RpcRequest)?
                            .attach_printable("failed to join ingestion task")
                            .change_context(IngestionError::RpcRequest)
                            .attach_printable("failed to ingest block")?;

                        info!(block = %block_info.cursor(), "ingested block");
                        self.chain_builder.grow(block_info).change_context(IngestionError::Model)?;

                        if self.chain_builder.segment_size() == self.options.chain_segment_size + self.options.chain_segment_upload_offset_size
                        {
                            let segment = self.chain_builder.take_segment(self.options.chain_segment_size).change_context(IngestionError::Model)?;
                            info!(first_block = %segment.info.first_block, "uploading chain segment");
                            self.chain_store.put(&segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;

                            let current_segment = self.chain_builder.current_segment().change_context(IngestionError::Model)?;
                            info!(first_block = %current_segment.info.first_block, "uploading recent chain segment");
                            let recent_etag = self.chain_store.put_recent(&current_segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;
                            self.state_client.put_ingested(recent_etag).await.change_context(IngestionError::StateClientRequest)?;
                        }
                    }

                    while self.can_push_task() {
                        trace!(block_number, "pushing finalized ingestion task");
                        self.push_ingest_block_by_number(block_number);
                        block_number += 1;
                    }
                }
            }
        }

        todo!();
    }

    fn can_push_task(&self) -> bool {
        self.task_queue.len() < self.options.max_concurrent_tasks
    }

    fn push_ingest_block_by_number(&mut self, block_number: u64) {
        self.task_queue.push_back(tokio::spawn({
            let ingestion = self.ingestion.clone();
            let store = self.block_store.clone();
            async move {
                let (block_info, block) = ingestion
                    .ingest_block_by_number(block_number)
                    .await
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to ingest block")
                    .attach_printable_lazy(|| format!("block number: {}", block_number))?;

                let block_cursor = block_info.cursor();
                debug!(cursor = %block_cursor, "uploading block");

                store
                    .put(&block_cursor, &block)
                    .await
                    .change_context(IngestionError::BlockStoreRequest)?;

                Ok(block_info)
            }
        }));
    }

    async fn get_starting_cursor(&mut self) -> Result<Cursor, IngestionError> {
        let recent_etag = self
            .state_client
            .get_ingested()
            .await
            .change_context(IngestionError::StateClientRequest)?;

        let existing_chain_segment = self
            .chain_store
            .get_recent(recent_etag)
            .await
            .change_context(IngestionError::CanonicalChainStoreRequest)
            .attach_printable("failed to get recent canonical chain segment")?;

        if let Some(existing_chain_segment) = existing_chain_segment {
            info!("restoring canonical chain from recent segment");
            self.chain_builder =
                CanonicalChainBuilder::restore_from_segment(existing_chain_segment)
                    .change_context(IngestionError::Model)
                    .attach_printable("failed to restore canonical chain from recent segment")?;
            let info = self.chain_builder.info().ok_or(IngestionError::Model)?;

            info!(first_block = %info.first_block, last_block = %info.last_block, "ingestion state restored");

            // TODO: check for offline reorgs.
            Ok(Cursor::new_finalized(info.last_block.number + 1))
        } else if let Some(starting_block) = self.options.override_starting_block {
            Ok(Cursor::new_finalized(starting_block))
        } else {
            Ok(Cursor::new_finalized(0))
        }
    }
}

impl Default for IngestionServiceOptions {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 100,
            chain_segment_size: 10_000,
            chain_segment_upload_offset_size: 100,
            override_starting_block: None,
        }
    }
}
