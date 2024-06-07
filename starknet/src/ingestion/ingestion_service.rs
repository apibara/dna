use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::{ChainChange, IngestionState, Snapshot, SnapshotChange},
    segment::SegmentOptions,
    storage::{LocalStorageBackend, StorageBackend},
};
use error_stack::ResultExt;
use futures_util::Stream;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{downloader::BlockDownloaderService, segmenter::SegmenterService};

use super::RpcProvider;

#[derive(Debug, Clone, Default)]
pub struct IngestionOptions {
    /// Segment creation options.
    pub segment: SegmentOptions,
    /// Start ingesting from this block number.
    pub starting_block: u64,
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
    <S as StorageBackend>::Writer: Send,
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

    pub async fn start(
        mut self,
        ct: CancellationToken,
    ) -> Result<impl Stream<Item = SnapshotChange>> {
        info!("Starting ingestion service");

        let snapshot = self.starting_snapshot().await?;
        info!(revision = snapshot.revision, "snapshot revision");

        let block_stream = BlockDownloaderService::new(
            self.provider.clone(),
            self.local_cache.clone(),
            self.chain_changes,
        )
        .start(snapshot.starting_block_number(), ct.clone());

        let snapshot_changes =
            SegmenterService::new(self.local_cache.clone(), self.storage, block_stream)
                .start(snapshot, ct.clone());

        Ok(snapshot_changes)
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

impl IngestionOptions {
    pub fn with_segment_options(mut self, segment: SegmentOptions) -> Self {
        self.segment = segment;
        self
    }

    pub fn with_starting_block(mut self, starting_block: u64) -> Self {
        self.starting_block = starting_block;
        self
    }
}
