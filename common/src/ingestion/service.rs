use std::{future::Future, sync::Arc, time::Duration};

use error_stack::{Result, ResultExt};
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::{task::JoinHandle, time::Interval};
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
    fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<BlockInfo, IngestionError>> + Send;

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
    /// How often to refresh the head block.
    pub head_refresh_interval: Duration,
    /// How often to refresh the finalized block.
    pub finalized_refresh_interval: Duration,
}

pub struct IngestionService<B, I>
where
    I: BlockIngestion<Block = B>,
    B: Send + Sync + 'static,
{
    options: IngestionServiceOptions,
    ingestion: IngestionInner<B, I>,
    state_client: IngestionStateClient,
    chain_store: ChainStore,
    chain_builder: CanonicalChainBuilder,
    task_queue: FuturesOrdered<IngestionTaskHandle>,
}

/// Wrap ingestion-related clients so we can clone them and push them to the task queue.
struct IngestionInner<B, I>
where
    I: BlockIngestion<Block = B>,
    B: Send + Sync + 'static,
{
    block_store: BlockStore<B>,
    ingestion: Arc<I>,
}

enum IngestionState {
    Ingest(IngestState),
    Recover,
}

struct IngestState {
    finalized: Cursor,
    head: Cursor,
    queued_block_number: u64,
    head_refresh_interval: Interval,
    finalized_refresh_interval: Interval,
}

/// What action to take when starting ingestion.
enum IngestionStartAction {
    /// Resume ingestion from the given cursor (cursor already ingested).
    Resume(Cursor),
    /// Start ingestion from the given block number (inclusive).
    Start(u64),
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
            ingestion: IngestionInner {
                ingestion: ingestion.into(),
                block_store,
            },
            state_client,
            chain_store,
            chain_builder: CanonicalChainBuilder::new(),
            task_queue: FuturesOrdered::new(),
        }
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<(), IngestionError> {
        let head = self.ingestion.get_head_cursor().await?;
        let finalized = self.ingestion.get_finalized_cursor().await?;

        self.state_client
            .put_finalized(finalized.number)
            .await
            .change_context(IngestionError::StateClientRequest)?;

        let mut state = match self.get_starting_cursor().await? {
            IngestionStartAction::Start(starting_block) => {
                // Ingest genesis block here so that the rest of the body is the same
                // as if we were resuming ingestion.
                info!(
                    starting_block = starting_block,
                    "starting ingestion from genesis block"
                );

                let block_info = self
                    .ingestion
                    .ingest_block_by_number(starting_block)
                    .await?;

                let starting_cursor = block_info.cursor();

                self.chain_builder
                    .grow(block_info)
                    .change_context(IngestionError::Model)?;

                info!(cursor = %starting_cursor, "uploaded genesis block");

                IngestionState::Ingest(IngestState {
                    queued_block_number: starting_cursor.number,
                    finalized,
                    head,
                    head_refresh_interval: tokio::time::interval(
                        self.options.head_refresh_interval,
                    ),
                    finalized_refresh_interval: tokio::time::interval(
                        self.options.finalized_refresh_interval,
                    ),
                })
            }
            IngestionStartAction::Resume(starting_cursor) => IngestionState::Ingest(IngestState {
                queued_block_number: starting_cursor.number,
                finalized,
                head,
                head_refresh_interval: tokio::time::interval(self.options.head_refresh_interval),
                finalized_refresh_interval: tokio::time::interval(
                    self.options.finalized_refresh_interval,
                ),
            }),
        };

        loop {
            if ct.is_cancelled() {
                break;
            }

            match state {
                IngestionState::Ingest(inner_state) => {
                    state = self.tick(inner_state, ct.clone()).await?;
                }
                IngestionState::Recover => {
                    // TODO: implement recovery.
                    return Err(IngestionError::Model)
                        .attach_printable("chain is in recovery state");
                }
            }
        }

        Ok(())
    }

    /// A single tick of ingestion.
    ///
    /// This is equivalent to `viewStep` in the Quint spec.
    async fn tick(
        &mut self,
        mut state: IngestState,
        ct: CancellationToken,
    ) -> Result<IngestionState, IngestionError> {
        tokio::select! {
            biased;

            _ = ct.cancelled() => Ok(IngestionState::Ingest(state)),

            _ = state.finalized_refresh_interval.tick() => {
                let finalized = self.ingestion.get_finalized_cursor().await.change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to refresh finalized cursor")?;

                if state.finalized.number > finalized.number {
                    return Err(IngestionError::Model)
                        .attach_printable("the new finalized cursor is behind the old one")
                        .attach_printable("this should never happen");
                }

                if state.finalized == finalized {
                    return Ok(IngestionState::Ingest(state));
                }

                info!(cursor = %finalized, "refreshed finalized cursor");

                self.state_client.put_finalized(finalized.number).await.change_context(IngestionError::StateClientRequest)?;

                Ok(IngestionState::Ingest(IngestState {
                    finalized,
                    ..state
                }))
            }

            _ = state.head_refresh_interval.tick() => {
                let head = self.ingestion.get_head_cursor().await.change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to refresh head cursor")?;

                if state.head == head {
                    return Ok(IngestionState::Ingest(state));
                }

                if state.head.number > head.number {
                    info!(old_head = %state.head, new_head = %head, "reorg detected");
                    return Ok(IngestionState::Recover);
                }

                if state.head.number == head.number && state.head.hash != head.hash {
                    return Ok(IngestionState::Recover);
                }

                info!(cursor = %head, "refreshed head cursor");

                Ok(IngestionState::Ingest(IngestState {
                    head,
                    ..state
                }))
            }

            join_result = self.task_queue.next() => {
                if let Some(join_result) = join_result {
                    let block_info = join_result
                        .change_context(IngestionError::RpcRequest)?
                        .attach_printable("failed to join ingestion task")
                        .change_context(IngestionError::RpcRequest)
                        .attach_printable("failed to ingest block")?;

                    info!(block = %block_info.cursor(), "ingested block");

                    // Always upload recent segment if the block is non-finalized.
                    let mut should_upload_recent_segment = block_info.number >= state.finalized.number;

                    if !self.chain_builder.can_grow(&block_info) {
                        return Ok(IngestionState::Recover);
                    }

                    self.chain_builder.grow(block_info).change_context(IngestionError::Model)?;

                    if self.chain_builder.segment_size() == self.options.chain_segment_size + self.options.chain_segment_upload_offset_size
                    {
                        let segment = self.chain_builder.take_segment(self.options.chain_segment_size).change_context(IngestionError::Model)?;
                        info!(first_block = %segment.info.first_block, "uploading chain segment");
                        self.chain_store.put(&segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;

                        should_upload_recent_segment = true;
                    }

                    if should_upload_recent_segment {
                        let current_segment = self.chain_builder.current_segment().change_context(IngestionError::Model)?;
                        info!(first_block = %current_segment.info.first_block, last_block = %current_segment.info.last_block, "uploading recent chain segment");
                        let recent_etag = self.chain_store.put_recent(&current_segment).await.change_context(IngestionError::CanonicalChainStoreRequest)?;
                        self.state_client.put_ingested(recent_etag).await.change_context(IngestionError::StateClientRequest)?;
                    }
                }

                let mut block_number = state.queued_block_number;
                while self.can_push_task() {
                    if block_number + 1 > state.head.number {
                        break;
                    }

                    block_number += 1;
                    trace!(block_number, "pushing finalized ingestion task");
                    self.push_ingest_block_by_number(block_number);
                }

                Ok(IngestionState::Ingest(IngestState {
                    queued_block_number: block_number,
                    ..state
                }))
            }
        }
    }

    fn can_push_task(&self) -> bool {
        self.task_queue.len() < self.options.max_concurrent_tasks
    }

    fn push_ingest_block_by_number(&mut self, block_number: u64) {
        let ingestion = self.ingestion.clone();
        self.task_queue.push_back(tokio::spawn(async move {
            ingestion.ingest_block_by_number(block_number).await
        }));
    }

    async fn get_starting_cursor(&mut self) -> Result<IngestionStartAction, IngestionError> {
        let recent_etag = self
            .state_client
            .get_ingested()
            .await
            .change_context(IngestionError::StateClientRequest)?;

        let existing_chain_segment = self
            .chain_store
            .get_recent(recent_etag.clone())
            .await
            .change_context(IngestionError::CanonicalChainStoreRequest)
            .attach_printable("failed to get recent canonical chain segment")?;

        if let Some(existing_chain_segment) = existing_chain_segment {
            if recent_etag.is_none() {
                return Err(IngestionError::Model)
                    .attach_printable("found recent chain segment but no etag in etcd")
                    .attach_printable("hint: did you use the correct prefixes?");
            }

            info!("restoring canonical chain from recent segment");
            self.chain_builder =
                CanonicalChainBuilder::restore_from_segment(existing_chain_segment)
                    .change_context(IngestionError::Model)
                    .attach_printable("failed to restore canonical chain from recent segment")?;
            let info = self.chain_builder.info().ok_or(IngestionError::Model)?;

            info!(first_block = %info.first_block, last_block = %info.last_block, "ingestion state restored");

            let block_info = self
                .ingestion
                .get_block_info_by_number(info.last_block.number)
                .await?;

            if info.last_block != block_info.cursor() {
                return Err(IngestionError::Model)
                    .attach_printable("last block in chain does not match last block in state")
                    .attach_printable("offline reorg not handled yet");
            }

            Ok(IngestionStartAction::Resume(block_info.cursor()))
        } else {
            let starting_block = self.options.override_starting_block.unwrap_or(0);

            self.state_client
                .put_starting_block(starting_block)
                .await
                .change_context(IngestionError::StateClientRequest)?;

            Ok(IngestionStartAction::Start(starting_block))
        }
    }
}

impl<B, I> IngestionInner<B, I>
where
    I: BlockIngestion<Block = B> + Send + Sync + 'static,
    B: Send + Sync + 'static + for<'a> Serializable<'a>,
{
    async fn ingest_block_by_number(&self, block_number: u64) -> Result<BlockInfo, IngestionError> {
        let ingestion = self.ingestion.clone();
        let store = self.block_store.clone();
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

    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        self.ingestion.get_head_cursor().await
    }

    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        self.ingestion.get_finalized_cursor().await
    }

    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        self.ingestion.get_block_info_by_number(block_number).await
    }
}

impl Default for IngestionServiceOptions {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 100,
            chain_segment_size: 10_000,
            chain_segment_upload_offset_size: 100,
            override_starting_block: None,
            head_refresh_interval: Duration::from_secs(3),
            finalized_refresh_interval: Duration::from_secs(30),
        }
    }
}

impl<B, I> Clone for IngestionInner<B, I>
where
    I: BlockIngestion<Block = B> + Send + Sync + 'static,
    B: Send + Sync + 'static + for<'a> Serializable<'a>,
{
    fn clone(&self) -> Self {
        Self {
            block_store: self.block_store.clone(),
            ingestion: self.ingestion.clone(),
        }
    }
}
