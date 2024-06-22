use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::{Blockifier, Segmenter, Snapshot, SnapshotManager},
    segment::SegmentArgs,
    storage::{CacheArgs, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{
    BeaconApiProvider, BeaconChainBlockIngestion, BeaconChainSegmentBuilder, ChainTracker,
};

use super::common::RpcArgs;

/// Start ingesting data from Ethereum.
///
/// If a snapshot is already present, it will be used to resume ingestion.
#[derive(Args, Debug)]
pub struct StartIngestionArgs {
    /// Ingestion server address.
    ///
    /// Defaults to `0.0.0.0:7001`.
    #[arg(long, env, default_value = "0.0.0.0:7001")]
    pub server_address: String,
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    /// Location for cached/temporary data.
    #[clap(flatten)]
    pub cache: CacheArgs,
    #[clap(flatten)]
    pub ingestion: IngestionArgs,
    #[clap(flatten)]
    pub rpc: RpcArgs,
}

#[derive(Args, Debug, Clone)]
pub struct IngestionArgs {
    /// Start ingesting data from this block, replacing any existing snapshot.
    #[arg(long, env)]
    pub starting_block: Option<u64>,
    #[clap(flatten)]
    pub segment: SegmentArgs,
}

pub async fn run_ingestion(args: StartIngestionArgs) -> Result<()> {
    info!("Starting Beacon Chain ingestion");
    let storage = args
        .storage
        .to_app_storage_backend()
        .attach_printable("failed to initialize storage backend")?;

    run_ingestion_with_storage(args, storage).await
}

async fn run_ingestion_with_storage<S>(args: StartIngestionArgs, storage: S) -> Result<()>
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
{
    let ct = CancellationToken::new();
    let local_cache_storage = args.cache.to_local_storage_backend();
    let segment_options = args.ingestion.segment.to_segment_options();

    let provider = BeaconApiProvider::new(&args.rpc.rpc_url);

    let chain_changes = ChainTracker::new(provider.clone()).start(ct.clone());

    let mut snapshot_manager = SnapshotManager::new(storage.clone());

    let starting_snapshot = snapshot_manager
        .read()
        .await
        .change_context(DnaError::Io)?
        .unwrap_or_else(|| {
            Snapshot::with_options(segment_options)
                .set_starting_block(args.ingestion.starting_block.unwrap_or(0))
        });

    let block_ingestion =
        BeaconChainBlockIngestion::new(provider.clone(), local_cache_storage.clone());

    let block_ingestion_stream = Blockifier::new(block_ingestion, chain_changes)
        .start(starting_snapshot.clone(), ct.clone());

    let segment_builder = BeaconChainSegmentBuilder {};

    let mut snapshot_changes = Segmenter::new(
        segment_builder,
        storage.clone(),
        snapshot_manager,
        block_ingestion_stream,
    )
    .start(starting_snapshot, ct.clone())
    .await
    .change_context(DnaError::Fatal)
    .attach_printable("failed to start segmenter")?;

    while let Some(change) = snapshot_changes.next().await {
        println!("{:?}", change);
    }

    todo!();
}
