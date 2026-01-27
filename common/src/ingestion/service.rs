use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use apibara_etcd::{EtcdClient, Lock};
use apibara_observability::{KeyValue, RecordRequest};
use error_stack::{Result, ResultExt};
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::{
    task::{JoinError, JoinHandle},
    time::Interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, field, info, trace, warn, Instrument};

use crate::{
    block_store::BlockStoreWriter,
    chain::{BlockInfo, CanonicalChainBuilder, CanonicalChainSegment, PendingBlockInfo},
    chain_store::ChainStore,
    file_cache::FileCache,
    fragment::Block,
    ingestion::IngestionErrorExt,
    object_store::ObjectStore,
    Cursor,
};

use super::{error::IngestionError, metrics::IngestionMetrics, state_client::IngestionStateClient};

pub trait BlockIngestion: Clone {
    fn supports_pending(&self) -> bool {
        false
    }

    fn get_head_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;

    fn get_finalized_cursor(&self) -> impl Future<Output = Result<Cursor, IngestionError>> + Send;

    fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<BlockInfo, IngestionError>> + Send;

    fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<(BlockInfo, Block), IngestionError>> + Send;

    fn ingest_pending_block(
        &self,
        _parent: &Cursor,
        _generation: u64,
    ) -> impl Future<Output = Result<Option<(PendingBlockInfo, Block)>, IngestionError>> + Send
    {
        async { Ok(None) }
    }
}

pub enum IngestionTask {
    Main(BlockInfo),
    Pending(Option<PendingBlockInfo>),
}

type IngestionTaskHandle = JoinHandle<Result<IngestionTask, IngestionError>>;

#[derive(Clone, Debug)]
pub struct IngestionServiceOptions {
    /// Maximum number of concurrent ingestion tasks.
    pub max_concurrent_tasks: usize,
    /// How many blocks in a single chain segment.
    pub chain_segment_size: usize,
    /// How many finalized blocks to wait before uploading a chain segment.
    pub chain_segment_upload_offset_size: usize,
    /// Override the ingestion starting block.
    pub override_starting_block: Option<u64>,
    /// How often to refresh the pending block.
    pub pending_refresh_interval: Duration,
    /// How often to refresh the head block.
    pub head_refresh_interval: Duration,
    /// How often to refresh the finalized block.
    pub finalized_refresh_interval: Duration,
}

pub struct IngestionService<I>
where
    I: BlockIngestion,
{
    options: IngestionServiceOptions,
    ingestion: IngestionInner<I>,
    state_client: IngestionStateClient,
    chain_store: ChainStore,
    chain_builder: CanonicalChainBuilder,
    task_queue: FuturesOrdered<IngestionTaskHandle>,
    metrics: IngestionMetrics,
}

pub type IngestionJobJoinResult =
    std::result::Result<Result<IngestionTask, IngestionError>, JoinError>;

/// Wrap ingestion-related clients so we can clone them and push them to the task queue.
#[derive(Clone)]
struct IngestionInner<I>
where
    I: BlockIngestion,
{
    block_store: BlockStoreWriter,
    ingestion: Arc<I>,
    metrics: IngestionMetrics,
}

#[derive(Debug)]
pub enum IngestionState {
    Ingest(IngestState),
    Recover(RecoverState),
}

#[derive(Debug, Default)]
struct PendingBlockState {
    queued: bool,
    generation: u64,
}

#[derive(Debug)]
pub struct IngestState {
    pub finalized: Cursor,
    pub head: Cursor,
    pub last_ingested: Cursor,
    pub queued_block_number: u64,
    pending_block_state: PendingBlockState,
    pending_refresh_interval: Interval,
    head_refresh_interval: Interval,
    finalized_refresh_interval: Interval,
}

#[derive(Debug)]
pub struct RecoverState {
    pub finalized: Cursor,
    pub existing_head: Cursor,
    pub last_ingested: Cursor,
}

/// What action to take when starting ingestion.
enum IngestionStartAction {
    /// Resume ingestion from the given cursor (cursor already ingested).
    Resume(Cursor),
    /// Start ingestion from the given block number (inclusive).
    Start(u64),
    /// Recover from an offline reorg.
    Recover(Cursor),
}

impl<I> IngestionService<I>
where
    I: BlockIngestion + Send + Sync + 'static,
{
    pub fn new(
        ingestion: I,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
        file_cache: FileCache,
        options: IngestionServiceOptions,
        metrics: IngestionMetrics,
    ) -> Self {
        let chain_store = ChainStore::new(object_store.clone(), file_cache);
        let block_store = BlockStoreWriter::new(object_store);
        let state_client = IngestionStateClient::new(&etcd_client);

        Self {
            options,
            ingestion: IngestionInner {
                ingestion: ingestion.into(),
                block_store,
                metrics: metrics.clone(),
            },
            state_client,
            chain_store,
            chain_builder: CanonicalChainBuilder::new(),
            task_queue: FuturesOrdered::new(),
            metrics,
        }
    }

    pub async fn start(
        mut self,
        lock: &mut Lock,
        ct: CancellationToken,
    ) -> Result<(), IngestionError> {
        let mut state = self.initialize().await?;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let tick_span = tracing::info_span!(
                "ingestion_tick",
                state_name = state.state_name(),
                head = field::Empty,
                finalized = field::Empty,
                task_queue_size = field::Empty,
                action = field::Empty,
            );

            lock.keep_alive()
                .await
                .change_context(IngestionError::LockKeepAlive)?;

            state = async {
                match state {
                    IngestionState::Ingest(inner_state) => {
                        self.tick_ingest(inner_state, ct.clone()).await
                    }
                    IngestionState::Recover(inner_state) => {
                        self.tick_recover(inner_state, ct.clone()).await
                    }
                }
            }
            .instrument(tick_span)
            .await?;

            state.record_metrics(&self.metrics);
        }
    }

    #[tracing::instrument(
        name = "ingestion_init",
        skip_all,
        err(Debug),
        fields(head, finalized, starting_block)
    )]
    pub async fn initialize(&mut self) -> Result<IngestionState, IngestionError> {
        debug!("initializing ingestion");
        let head = self.ingestion.get_head_cursor().await?;
        let finalized = self.ingestion.get_finalized_cursor().await?;

        let current_span = tracing::Span::current();

        current_span.record("head", head.number);
        current_span.record("finalized", finalized.number);
        debug!(head = %head, finalized = %finalized, "received head and finalized blocks");

        self.state_client
            .put_finalized(finalized.number)
            .await
            .change_context(IngestionError::StateClientRequest)?;

        match self.get_starting_cursor().await? {
            IngestionStartAction::Recover(last_ingested) => {
                Ok(IngestionState::Recover(RecoverState {
                    finalized,
                    existing_head: head,
                    last_ingested,
                }))
            }
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

                current_span.record("starting_block", starting_block);

                info!(cursor = %starting_cursor, "uploaded genesis block");

                Ok(IngestionState::Ingest(IngestState {
                    queued_block_number: starting_cursor.number,
                    finalized,
                    head,
                    last_ingested: starting_cursor,
                    pending_block_state: PendingBlockState::default(),
                    pending_refresh_interval: tokio::time::interval(
                        self.options.pending_refresh_interval,
                    ),
                    head_refresh_interval: tokio::time::interval(
                        self.options.head_refresh_interval,
                    ),
                    finalized_refresh_interval: tokio::time::interval(
                        self.options.finalized_refresh_interval,
                    ),
                }))
            }
            IngestionStartAction::Resume(starting_cursor) => {
                current_span.record("starting_block", starting_cursor.number);

                Ok(IngestionState::Ingest(IngestState {
                    queued_block_number: starting_cursor.number,
                    finalized,
                    head,
                    last_ingested: starting_cursor,
                    pending_block_state: PendingBlockState::default(),
                    pending_refresh_interval: tokio::time::interval(
                        self.options.pending_refresh_interval,
                    ),
                    head_refresh_interval: tokio::time::interval(
                        self.options.head_refresh_interval,
                    ),
                    finalized_refresh_interval: tokio::time::interval(
                        self.options.finalized_refresh_interval,
                    ),
                }))
            }
        }
    }

    /// A single tick of ingestion.
    ///
    /// This is equivalent to `viewStep` in the Quint spec.
    async fn tick_ingest(
        &mut self,
        mut state: IngestState,
        ct: CancellationToken,
    ) -> Result<IngestionState, IngestionError> {
        let current_span = tracing::Span::current();

        current_span.record("head", state.head.number);
        current_span.record("finalized", state.finalized.number);
        current_span.record("task_queue_size", self.task_queue.len());

        tokio::select! {
            biased;

            _ = ct.cancelled() => Ok(IngestionState::Ingest(state)),

            _ = state.finalized_refresh_interval.tick() => {
                current_span.record("action", "refresh_finalized");

                self.tick_refresh_finalized(state).await
            }

            _ = state.pending_refresh_interval.tick(), if state.head == state.last_ingested && self.ingestion.supports_pending() => {
                current_span.record("action", "refresh_pending");

                self.tick_refresh_pending(state).await
            }

            _ = state.head_refresh_interval.tick() => {
                current_span.record("action", "refresh_head");

                self.tick_refresh_head(state).await
            }

            join_result = self.task_queue_next(), if !self.task_queue_is_empty() => {
                current_span.record("action", "finish_ingestion");

                self.tick_with_task_result(state, join_result).await
            }
        }
    }

    pub async fn tick_refresh_finalized(
        &mut self,
        state: IngestState,
    ) -> Result<IngestionState, IngestionError> {
        let finalized = self
            .ingestion
            .get_finalized_cursor()
            .await
            .change_context(IngestionError::RpcRequest)
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

        self.state_client
            .put_finalized(finalized.number)
            .await
            .change_context(IngestionError::StateClientRequest)?;

        Ok(IngestionState::Ingest(IngestState { finalized, ..state }))
    }

    pub async fn tick_refresh_head(
        &mut self,
        mut state: IngestState,
    ) -> Result<IngestionState, IngestionError> {
        let head = self
            .ingestion
            .get_head_cursor()
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to refresh head cursor")?;

        if state.head == head {
            return Ok(IngestionState::Ingest(state));
        }

        // Reset the pending refresh interval so that we don't ingest pending data too early.
        state.pending_refresh_interval.reset();

        // Change of heads that are not ingested are not important.
        if state.last_ingested.number >= head.number {
            if state.head.number > head.number {
                info!(old_head = %state.head, new_head = %head, "reorg detected");
                return Ok(IngestionState::Recover(RecoverState {
                    finalized: state.finalized,
                    existing_head: state.head,
                    last_ingested: state.last_ingested,
                }));
            }

            if state.head.number == head.number && state.head.hash != head.hash {
                return Ok(IngestionState::Recover(RecoverState {
                    finalized: state.finalized,
                    existing_head: state.head,
                    last_ingested: state.last_ingested,
                }));
            }
        }

        info!(cursor = %head, "refreshed head cursor");

        let mut block_number = state.queued_block_number;
        while self.can_push_task() {
            if block_number + 1 > head.number {
                break;
            }

            block_number += 1;
            trace!(block_number, "pushing finalized ingestion task");
            self.push_ingest_block_by_number(block_number);
        }

        Ok(IngestionState::Ingest(IngestState {
            head,
            queued_block_number: block_number,
            ..state
        }))
    }

    pub async fn tick_refresh_pending(
        &mut self,
        mut state: IngestState,
    ) -> Result<IngestionState, IngestionError> {
        if state.pending_block_state.queued {
            return Ok(IngestionState::Ingest(state));
        }

        // Only ingest pending blocks if they will be used.
        if state.head != state.last_ingested {
            return Ok(IngestionState::Ingest(state));
        }

        self.push_ingest_pending_block(
            state.last_ingested.clone(),
            state.pending_block_state.generation + 1,
        );

        state.pending_block_state.queued = true;

        Ok(IngestionState::Ingest(state))
    }

    pub async fn tick_with_task_result(
        &mut self,
        state: IngestState,
        join_result: Option<IngestionJobJoinResult>,
    ) -> Result<IngestionState, IngestionError> {
        let mut last_ingested = state.last_ingested.clone();

        if let Some(join_result) = join_result {
            let task_result = join_result
                .change_context(IngestionError::RpcRequest)
                .attach_printable("failed to join ingestion task")?;

            let block_info = match task_result {
                Ok(block_info) => block_info,
                Err(err) if err.is_block_not_found() => {
                    return Ok(IngestionState::Recover(RecoverState {
                        finalized: state.finalized,
                        existing_head: state.head,
                        last_ingested: state.last_ingested,
                    }));
                }
                Err(err) => {
                    return Err(err)
                        .change_context(IngestionError::RpcRequest)
                        .attach_printable("failed to ingest block")
                }
            };

            let block_info = match block_info {
                IngestionTask::Main(block_info) => block_info,
                IngestionTask::Pending(None) => {
                    let new_pending_block_state = PendingBlockState {
                        queued: false,
                        generation: state.pending_block_state.generation,
                    };

                    return Ok(IngestionState::Ingest(IngestState {
                        pending_block_state: new_pending_block_state,
                        ..state
                    }));
                }
                IngestionTask::Pending(Some(block_info)) => {
                    info!(
                        number = block_info.number,
                        generation = block_info.generation,
                        "ingested pending block"
                    );

                    self.state_client
                        .put_pending(block_info.generation)
                        .await
                        .change_context(IngestionError::StateClientRequest)?;

                    let new_pending_block_state = PendingBlockState {
                        queued: false,
                        generation: block_info.generation,
                    };

                    return Ok(IngestionState::Ingest(IngestState {
                        pending_block_state: new_pending_block_state,
                        ..state
                    }));
                }
            };

            info!(block = %block_info.cursor(), "ingested block");

            // Always upload recent segment if the block is non-finalized.
            let mut should_upload_recent_segment = block_info.number >= state.finalized.number;

            if !self.chain_builder.can_grow(&block_info) {
                return Ok(IngestionState::Recover(RecoverState {
                    finalized: state.finalized,
                    existing_head: state.head,
                    last_ingested: state.last_ingested,
                }));
            }

            last_ingested = block_info.cursor();

            self.chain_builder
                .grow(block_info)
                .change_context(IngestionError::Model)?;

            if self.chain_builder.segment_size()
                >= self.options.chain_segment_size + self.options.chain_segment_upload_offset_size
            {
                let segment = self
                    .chain_builder
                    .take_segment(self.options.chain_segment_size)
                    .change_context(IngestionError::Model)?;
                info!(first_block = %segment.info.first_block, "uploading chain segment");
                self.chain_store
                    .put(&segment)
                    .await
                    .change_context(IngestionError::CanonicalChainStoreRequest)?;

                should_upload_recent_segment = true;
            }

            if should_upload_recent_segment {
                let current_segment = self
                    .chain_builder
                    .current_segment()
                    .change_context(IngestionError::Model)?;
                info!(first_block = %current_segment.info.first_block, last_block = %current_segment.info.last_block, "uploading recent chain segment");
                let recent_etag = self
                    .chain_store
                    .put_recent(&current_segment)
                    .await
                    .change_context(IngestionError::CanonicalChainStoreRequest)?;
                self.state_client
                    .put_ingested(recent_etag)
                    .await
                    .change_context(IngestionError::StateClientRequest)?;
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
            last_ingested,
            queued_block_number: block_number,
            pending_block_state: PendingBlockState::default(),
            ..state
        }))
    }

    pub async fn tick_recover(
        &mut self,
        state: RecoverState,
        ct: CancellationToken,
    ) -> Result<IngestionState, IngestionError> {
        let current_span = tracing::Span::current();
        current_span.record("action", "recover");

        warn!(last_ingested = %state.last_ingested, "recovering from a chain reorganization");

        let canonical_chain = self
            .chain_builder
            .current_segment()
            .change_context(IngestionError::Model)?;

        let last_ingested = canonical_chain
            .canonical(state.last_ingested.number)
            .change_context(IngestionError::Model)?;

        // This should never happen but we check just in case.
        if last_ingested != state.last_ingested {
            return Err(IngestionError::Model)
                .attach_printable("last ingested block does not match canonical chain");
        }

        let mut new_head_candidate = state.last_ingested.clone();

        loop {
            // Give up and exit.
            if ct.is_cancelled() {
                return Ok(IngestionState::Recover(state));
            }

            match self
                .ingestion
                .get_block_info_by_number(new_head_candidate.number)
                .await
            {
                Ok(remote_block_info) => {
                    if remote_block_info.cursor() == new_head_candidate {
                        break;
                    }
                }
                Err(err) if err.is_block_not_found() => {
                    // This block doesn't exist anymore.
                    // This can happen in case of reorgs.
                }
                Err(err) => {
                    return Err(err)
                        .change_context(IngestionError::RpcRequest)
                        .attach_printable("failed to get block info while recovering")
                        .attach_printable_lazy(|| {
                            format!("block number: {}", new_head_candidate.number)
                        })
                }
            };

            if new_head_candidate.number == 0 {
                return Err(IngestionError::Model)
                    .attach_printable("failed to recover from genesis block")
                    .attach_printable("hint: are you connecting to the correct chain?");
            }

            new_head_candidate = canonical_chain
                .canonical(new_head_candidate.number - 1)
                .change_context(IngestionError::Model)
                .attach_printable("failed to get parent block in reorg recovery")
                .attach_printable_lazy(|| {
                    format!("current block number: {}", new_head_candidate.number)
                })?;
        }

        self.task_queue_clear();
        self.chain_builder
            .shrink(new_head_candidate.clone())
            .change_context(IngestionError::Model)
            .attach_printable("failed to shrink canonical chain after reorg recovery")
            .attach_printable_lazy(|| format!("new head: {}", new_head_candidate))?;

        info!(new_head = %new_head_candidate, "recovered from a chain reorganization");

        Ok(IngestionState::Ingest(IngestState {
            finalized: state.finalized,
            head: state.existing_head,
            queued_block_number: new_head_candidate.number,
            last_ingested: new_head_candidate,
            pending_block_state: PendingBlockState::default(),
            pending_refresh_interval: tokio::time::interval(self.options.pending_refresh_interval),
            head_refresh_interval: tokio::time::interval(self.options.head_refresh_interval),
            finalized_refresh_interval: tokio::time::interval(
                self.options.finalized_refresh_interval,
            ),
        }))
    }

    pub fn task_queue_clear(&mut self) {
        self.task_queue = FuturesOrdered::new();
    }

    pub fn task_queue_len(&self) -> usize {
        self.task_queue.len()
    }

    pub fn task_queue_is_empty(&self) -> bool {
        self.task_queue.is_empty()
    }

    pub fn task_queue_next(&mut self) -> impl Future<Output = Option<IngestionJobJoinResult>> + '_ {
        self.task_queue.next()
    }

    pub fn can_push_task(&self) -> bool {
        self.task_queue.len() < self.options.max_concurrent_tasks
    }

    pub fn push_ingest_block_by_number(&mut self, block_number: u64) {
        let ingestion = self.ingestion.clone();
        self.task_queue.push_back(tokio::spawn(async move {
            let block_info = ingestion.ingest_block_by_number(block_number).await?;
            Ok(IngestionTask::Main(block_info))
        }));
    }

    pub fn push_ingest_pending_block(&mut self, last_ingested: Cursor, generation: u64) {
        let ingestion = self.ingestion.clone();
        self.task_queue.push_back(tokio::spawn(async move {
            let block_info = ingestion
                .ingest_pending_block(last_ingested, generation)
                .await?;
            Ok(IngestionTask::Pending(block_info))
        }));
    }

    pub fn current_chain_segment(&self) -> Option<CanonicalChainSegment> {
        self.chain_builder.current_segment().ok()
    }

    async fn get_starting_cursor(&mut self) -> Result<IngestionStartAction, IngestionError> {
        let existing_chain_segment = self
            .chain_store
            .get_recent(None)
            .await
            .change_context(IngestionError::CanonicalChainStoreRequest)
            .attach_printable("failed to get recent canonical chain segment")?;

        if let Some(existing_chain_segment) = existing_chain_segment {
            info!("restoring canonical chain");
            self.chain_builder =
                CanonicalChainBuilder::restore_from_segment(existing_chain_segment)
                    .change_context(IngestionError::Model)
                    .attach_printable("failed to restore canonical chain from recent segment")?;
            let info = self.chain_builder.info().ok_or(IngestionError::Model)?;

            info!(first_block = %info.first_block, last_block = %info.last_block, "ingestion state restored");

            let block_info = match self
                .ingestion
                .get_block_info_by_number(info.last_block.number)
                .await
                .attach_printable("failed to get starting cursor block info")
            {
                Ok(block_info) => block_info,
                Err(err) if err.is_block_not_found() => {
                    return Ok(IngestionStartAction::Recover(info.last_block.clone()));
                }
                Err(err) => return Err(err).change_context(IngestionError::RpcRequest),
            };

            if info.last_block != block_info.cursor() {
                return Ok(IngestionStartAction::Recover(info.last_block.clone()));
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

impl<I> IngestionInner<I>
where
    I: BlockIngestion + Send + Sync + 'static,
{
    fn supports_pending(&self) -> bool {
        self.ingestion.supports_pending()
    }

    #[tracing::instrument("ingestion_ingest_block", skip(self), err(Debug))]
    async fn ingest_block_by_number(&self, block_number: u64) -> Result<BlockInfo, IngestionError> {
        let ingestion_start_time = Instant::now();

        let ingestion = self.ingestion.clone();
        let store = self.block_store.clone();
        let rpc_metrics = self.metrics.rpc.clone();

        let (block_info, block) = ingestion
            .ingest_block_by_number(block_number)
            .record_request_with_attributes(
                rpc_metrics,
                &[KeyValue::new("method", "ingest_block_by_number")],
            )
            .await
            .attach_printable_lazy(|| format!("block number: {}", block_number))?;

        if block.index.len() != block.body.len() {
            return Err(IngestionError::Model)
                .attach_printable("block indexes and body fragments do not match")
                .attach_printable_lazy(|| format!("block number: {}", block_number))
                .attach_printable_lazy(|| format!("indexes len: {}", block.index.len()))
                .attach_printable_lazy(|| format!("body len: {}", block.body.len()));
        }

        let block_cursor = block_info.cursor();
        debug!(cursor = %block_cursor, "uploading block");

        let block_upload_metrics = self.metrics.block_upload.clone();

        let (size, _etag) = store
            .put_block(&block_cursor, &block)
            .record_request_with_attributes(
                block_upload_metrics,
                &[KeyValue::new("method", "ingest_block_by_number")],
            )
            .await
            .change_context(IngestionError::BlockStoreRequest)?;

        let ingestion_elapsed = ingestion_start_time.elapsed();
        self.metrics
            .ingestion_latency
            .record(ingestion_elapsed.as_secs_f64(), &[]);

        self.metrics
            .block_size
            .record(size as u64, &[KeyValue::new("type", "produced")]);

        Ok(block_info)
    }

    #[tracing::instrument("ingestion_ingest_pending_block", skip(self), err(Debug))]
    async fn ingest_pending_block(
        &self,
        parent: Cursor,
        generation: u64,
    ) -> Result<Option<PendingBlockInfo>, IngestionError> {
        let block_number = parent.number + 1;
        let ingestion = self.ingestion.clone();
        let store = self.block_store.clone();
        let rpc_metrics = self.metrics.rpc.clone();

        let Some((block_info, block)) = ingestion
            .ingest_pending_block(&parent, generation)
            .record_request_with_attributes(
                rpc_metrics,
                &[KeyValue::new("method", "ingest_pending_block")],
            )
            .await
            .attach_printable_lazy(|| format!("block number: {}", block_number))?
        else {
            return Ok(None);
        };

        if block.index.len() != block.body.len() {
            return Err(IngestionError::Model)
                .attach_printable("block indexes and body fragments do not match (pending block)")
                .attach_printable_lazy(|| format!("block number: {}", block_number))
                .attach_printable_lazy(|| format!("indexes len: {}", block.index.len()))
                .attach_printable_lazy(|| format!("body len: {}", block.body.len()));
        }

        debug!(cursor = ?block_info, "uploading pending block");

        let block_upload_metrics = self.metrics.block_upload.clone();

        let (size, _etag) = store
            .put_pending_block(&block_info, &block)
            .record_request_with_attributes(
                block_upload_metrics,
                &[KeyValue::new("method", "ingest_pending_block")],
            )
            .await
            .change_context(IngestionError::BlockStoreRequest)?;

        self.metrics
            .block_size
            .record(size as u64, &[KeyValue::new("type", "pending")]);

        Ok(block_info.into())
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
            pending_refresh_interval: Duration::from_secs(3),
            head_refresh_interval: Duration::from_secs(3),
            finalized_refresh_interval: Duration::from_secs(30),
        }
    }
}

impl IngestionState {
    pub fn state_name(&self) -> &'static str {
        match self {
            IngestionState::Recover(_) => "recover",
            IngestionState::Ingest(_) => "ingest",
        }
    }

    pub fn is_ingested(&self) -> bool {
        matches!(self, IngestionState::Ingest(_))
    }

    pub fn is_recover(&self) -> bool {
        matches!(self, IngestionState::Recover(_))
    }

    pub fn as_ingest(&self) -> Option<&IngestState> {
        match self {
            IngestionState::Ingest(state) => Some(state),
            _ => None,
        }
    }

    pub fn as_recover(&self) -> Option<&RecoverState> {
        match self {
            IngestionState::Recover(state) => Some(state),
            _ => None,
        }
    }

    pub fn take_ingest(self) -> Option<IngestState> {
        match self {
            IngestionState::Ingest(state) => Some(state),
            _ => None,
        }
    }

    pub fn take_recover(self) -> Option<RecoverState> {
        match self {
            IngestionState::Recover(state) => Some(state),
            _ => None,
        }
    }

    pub fn record_metrics(&self, metrics: &IngestionMetrics) {
        match self {
            IngestionState::Ingest(state) => {
                metrics.state.record(1, &[]);
                metrics.head.record(state.head.number, &[]);
                metrics.ingested.record(state.last_ingested.number, &[]);
                metrics.finalized.record(state.finalized.number, &[]);
            }
            IngestionState::Recover(state) => {
                metrics.state.record(2, &[]);
                metrics.ingested.record(state.last_ingested.number, &[]);
                metrics.finalized.record(state.finalized.number, &[]);
            }
        }
    }
}
