use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::{IngestionServer, IngestionState, Snapshot},
    segment::SegmentOptions,
    storage::{LocalStorageBackend, StorageBackend},
};
use error_stack::ResultExt;
use futures_util::Stream;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{downloader::BlockDownloaderService, segmenter::SegmenterService};

use super::{ChainChange, RpcProvider};

#[derive(Debug, Clone)]
pub struct RpcIngestionOptions {
    /// Fetch transactions for each block in a single call.
    pub get_block_by_number_with_transactions: bool,
    /// Use `eth_getBlockReceipts` instead of `eth_getTransactionReceipt`.
    pub get_block_receipts_by_number: bool,
}

#[derive(Debug, Clone)]
pub struct IngestionOptions {
    /// Segment creation options.
    pub segment: SegmentOptions,
    /// Start ingesting from this block number.
    pub starting_block: u64,
    /// Control how to ingest blocks from the RPC.
    pub rpc_ingestion: RpcIngestionOptions,
}

pub struct IngestionService<
    S: StorageBackend + Send + Sync + 'static,
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
> {
    provider: RpcProvider,
    local_cache: LocalStorageBackend,
    storage: S,
    chain_changes: C,
    options: IngestionOptions,
}

impl<S, C> IngestionService<S, C>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        provider: RpcProvider,
        local_cache: LocalStorageBackend,
        storage: S,
        chain_changes: C,
    ) -> Self {
        Self {
            provider,
            local_cache,
            storage,
            chain_changes,
            options: IngestionOptions::default(),
        }
    }

    pub fn with_options(mut self, options: IngestionOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn start(mut self, ct: CancellationToken) -> Result<()> {
        info!("Starting ingestion service");

        let snapshot = self.starting_snapshot().await?;
        info!(revision = snapshot.revision, "snapshot revision");

        let block_stream = BlockDownloaderService::new(
            self.provider.clone(),
            self.local_cache.clone(),
            self.chain_changes,
            self.options.rpc_ingestion.clone(),
        )
        .start(snapshot.starting_block_number(), ct.clone());

        let snapshot_changes =
            SegmenterService::new(self.local_cache.clone(), self.storage, block_stream)
                .start(snapshot, ct.clone());

        let address = "0.0.0.0:7000".parse().expect("parse address");
        IngestionServer::new(self.local_cache.clone(), snapshot_changes)
            .start(address, ct)
            .await
    }

    /// Read the starting snapshot from storage, if any.
    async fn starting_snapshot(&mut self) -> Result<Snapshot> {
        if self.storage.exists("", "snapshot").await? {
            let mut reader = self.storage.get("", "snapshot").await?;
            let mut buf = String::new();
            reader
                .read_to_string(&mut buf)
                .await
                .change_context(DnaError::Io)
                .attach_printable("failed to read snapshot")?;
            let snapshot = Snapshot::from_str(&buf)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read snapshot")?;
            Ok(snapshot)
        } else {
            let snapshot = Snapshot {
                revision: 0,
                segment_options: self.options.segment.clone(),
                ingestion: IngestionState {
                    first_block_number: self.options.starting_block,
                    group_count: 0,
                    extra_segment_count: 0,
                },
            };
            Ok(snapshot)
        }
    }
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            segment: SegmentOptions::default(),
            starting_block: 0,
            rpc_ingestion: RpcIngestionOptions::default(),
        }
    }
}

impl Default for RpcIngestionOptions {
    fn default() -> Self {
        Self {
            get_block_by_number_with_transactions: true,
            get_block_receipts_by_number: false,
        }
    }
}

impl IngestionOptions {
    pub fn with_segment_options(mut self, segment: SegmentOptions) -> Self {
        self.segment = segment;
        self
    }

    pub fn with_starting_block(mut self, starting_block: u64) -> Self {
        self.starting_block = starting_block;
        self
    }

    pub fn with_rpc_ingestion(mut self, rpc_ingestion: RpcIngestionOptions) -> Self {
        self.rpc_ingestion = rpc_ingestion;
        self
    }
}

impl RpcIngestionOptions {
    pub fn with_get_block_by_number_with_transactions(mut self, flag: bool) -> Self {
        self.get_block_by_number_with_transactions = flag;
        self
    }

    pub fn with_get_block_receipts_by_number(mut self, flag: bool) -> Self {
        self.get_block_receipts_by_number = flag;
        self
    }
}
/*
use std::future;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::{IngestionEvent, Snapshot},
    segment::SegmentOptions,
    storage::StorageBackend,
};
use error_stack::ResultExt;
use futures_util::{FutureExt, Stream, StreamExt, TryFutureExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::ingestion::{downloader::BlockDownloader, segmenter::Segmenter};

use super::{ChainChange, RpcProvider};

#[derive(Debug, Clone)]
pub struct IngestorOptions {
    /// Segment creation options.
    pub segment: SegmentOptions,
    /// Start ingesting from this block number.
    pub starting_block: u64,
    /// Fetch transactions for each block in a single call.
    pub get_block_by_number_with_transactions: bool,
    /// Use `eth_getBlockReceipts` instead of `eth_getTransactionReceipt`.
    pub get_block_receipts_by_number: bool,
}

pub struct Ingestor<
    S: StorageBackend + Send + Sync + 'static,
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
> {
    provider: RpcProvider,
    storage: S,
    chain_changes: C,
    options: IngestorOptions,
}

impl<S, C> Ingestor<S, C>
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Unpin + Send,
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(provider: RpcProvider, storage: S, chain_changes: C) -> Self {
        Self {
            provider,
            storage,
            chain_changes,
            options: IngestorOptions::default(),
        }
    }

    pub fn with_options(mut self, options: IngestorOptions) -> Self {
        self.options = options;
        self
    }

    pub fn start(self, ct: CancellationToken) -> impl Stream<Item = IngestionEvent> {
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(
            ingest_chain_data(
                self.provider,
                self.storage,
                self.chain_changes,
                self.options,
                tx,
                ct,
            )
            .inspect_err(|err| {
                error!(err = ?err, "ingestor loop returned with error");
            }),
        );
        ReceiverStream::new(rx)
    }
}

/// Ingest chain data.
///
/// This function doesn't ingest data, but it orchestrates the ingestion
/// process.
/// It listens for chain changes and triggers the ingestion worker to
/// ingest blocks. The worker is responsible for generating segments and
/// writing them to storage.
async fn ingest_chain_data<S, C>(
    provider: RpcProvider,
    storage: S,
    mut chain_changes: C,
    options: IngestorOptions,
    tx: mpsc::Sender<IngestionEvent>,
    ct: CancellationToken,
) -> Result<()>
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Unpin + Send,
    C: Stream<Item = ChainChange> + Unpin + Send + Sync + 'static,
{
    // 1) Listen to initialize chain event.
    let (mut head, mut finalized) = match chain_changes.next().await {
        Some(ChainChange::Initialize { head, finalized }) => (head, finalized),
        _ => {
            return Err(DnaError::Fatal).attach_printable("expected chain initialization");
        }
    };

    info!(
        head = head.number,
        finalized = finalized.number,
        "initialized chain"
    );

    // 2) Setup block downloader and segmenter.
    let downloader = BlockDownloader::new(provider, storage.clone(), options.clone());
    let segmenter = Segmenter::new(storage, options).await?;

    let starting_state = segmenter.snapshot_state().clone();

    {
        let snapshot = Snapshot {
            revision: starting_state.revision,
            first_block_number: starting_state.first_block_number,
            segment_options: starting_state.segment_options.clone(),
            group_count: starting_state.group_count,
        };

        tx.send(IngestionEvent::Started(snapshot))
            .await
            .map_err(|_| DnaError::Fatal)
            .attach_printable("failed to send ingestion event: started")?;
    }

    let mut current_block_number = starting_state.starting_block();

    info!(
        revision = starting_state.revision,
        segment_options = ?starting_state.segment_options,
        starting_block_number = current_block_number,
        "starting ingestion"
    );

    // 3) Start ingesting blocks.
    let mut downloader_fut = future::pending().fuse().boxed();
    let mut downloader_state = WorkerState::Available;

    let mut segmenter_fut = future::pending().fuse().boxed();
    let mut segmenter_state = WorkerState::Available;

    loop {
        if !downloader_state.is_busy() {
            if head.number >= current_block_number {
                downloader_fut = downloader
                    .download_block(current_block_number.into())
                    .boxed();
                downloader_state = WorkerState::Busy;
            } else if downloader_state.is_available() {
                info!("reached finalized block. waiting.");
                // Wait until the block moves to continue ingesting.
                downloader_fut = future::pending().fuse().boxed();
                downloader_state = WorkerState::Waiting;
            }
        }

        if !segmenter_state.is_busy() {}

        tokio::select! {
            result = &mut downloader_fut => {
                current_block_number += 1;
                downloader_state = WorkerState::Available;
            }

            result = &mut segmenter_fut => {
                println!("got result {:?}", result);
                segmenter_state = WorkerState::Available;
            }

            Some(chain_change) = chain_changes.next() => {
                match chain_change {
                    ChainChange::NewFinalized(new_finalized) => {
                        info!(block = ?new_finalized, "new finalized");
                        finalized = new_finalized;
                    }
                    ChainChange::NewHead(new_head) => {
                        info!(block = ?new_head, "new head");
                        head = new_head;
                    }
                    _ => {}
                }
            }
            _ = ct.cancelled() => {
                break;
            }
            else => {
                break;
            }
        };
    }

    // 2) Start ingestion worker.
    /*
    let (worker, starting_state) = IngestionWorkerBuilder::new(provider, storage, options)
        .start(ct.clone())
        .await?;

    let mut block_number = starting_state.starting_block();
    let mut finalized = finalized.number;

    {
        let snapshot = Snapshot {
            revision: starting_state.revision,
            first_block_number: starting_state.first_block_number,
            segment_options: starting_state.segment_options.clone(),
            group_count: starting_state.group_count,
        };

        tx.send(IngestionEvent::Started(snapshot))
            .await
            .map_err(|_| DnaError::Fatal)
            .attach_printable("failed to send ingestion event: started")?;
    }

    info!(
        revision = starting_state.revision,
        segment_options = ?starting_state.segment_options,
        starting_block_number = block_number,
        "starting ingestion"
    );

    // 3) Start ingesting blocks.
    let mut worker_fut = future::pending().fuse().boxed();
    let mut worker_state = WorkerState::Available;

    loop {
        if !worker_state.is_busy() {
            if finalized >= block_number {
                worker_fut = worker.ingest_block_by_number(block_number).boxed();
                worker_state = WorkerState::Busy;
            } else if worker_state.is_available() {
                info!("reached finalized block. waiting.");
                // Wait until the block moves to continue ingesting.
                worker_fut = future::pending().fuse().boxed();
                worker_state = WorkerState::Waiting;
            }
        }

        tokio::select! {
            result = &mut worker_fut => {
                let result = result?;
                if let Some(event) = result {
                    tx.send(event)
                        .await
                        .map_err(|_| DnaError::Fatal)
                        .attach_printable("failed to send ingestion event")?;
                }
                block_number += 1;
                worker_state = WorkerState::Available;
            }
            Some(chain_change) = chain_changes.next() => {
                match chain_change {
                    ChainChange::NewFinalized(new_finalized) => {
                        info!(block = ?new_finalized, "new finalized");
                        if new_finalized.number > finalized {
                            finalized = new_finalized.number;
                        }
                    }
                    ChainChange::NewHead(new_head) => {
                        info!(block = ?new_head, "new head");
                    }
                    _ => {}
                }
            }
            _ = ct.cancelled() => {
                break;
            }
            else => {
                break;
            }
        };
    }

    */
    todo!();
    Ok(())
}

impl Default for IngestorOptions {
    fn default() -> Self {
        Self {
            segment: SegmentOptions::default(),
            starting_block: 0,
            get_block_by_number_with_transactions: true,
            get_block_receipts_by_number: false,
        }
    }
}

enum WorkerState {
    /// Worker is busy ingesting a block.
    Busy,
    /// Worker is available to ingest a block.
    Available,
    /// Worker is waiting for the head block to move.
    Waiting,
}

impl WorkerState {
    fn is_busy(&self) -> bool {
        matches!(self, WorkerState::Busy)
    }

    fn is_available(&self) -> bool {
        matches!(self, WorkerState::Available)
    }
}

*/
