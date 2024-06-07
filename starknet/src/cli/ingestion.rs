use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::IngestionServer,
    segment::SegmentArgs,
    storage::{CacheArgs, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{ChainTracker, IngestionOptions, IngestionService};

use super::common::RpcArgs;

/// Start ingesting data from Starknet.
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
    info!("Starting Starknet ingestion");
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

    let provider = args.rpc.to_provider_service()?.start(ct.clone());
    let chain_changes = ChainTracker::new(provider.clone()).start(ct.clone());

    let local_cache_storage = args.cache.to_local_storage_backend();

    let ingestion_stream = IngestionService::new(
        provider,
        local_cache_storage.clone(),
        storage,
        chain_changes,
    )
    .with_options(args.ingestion.to_ingestion_options())
    .start(ct.clone())
    .await?;

    let address = args
        .server_address
        .parse::<SocketAddr>()
        .change_context(DnaError::Configuration)
        .attach_printable_lazy(|| format!("failed to parse address: {}", args.server_address))?;

    IngestionServer::new(local_cache_storage, ingestion_stream)
        .start(address, ct)
        .await
}

impl IngestionArgs {
    pub fn to_ingestion_options(&self) -> IngestionOptions {
        IngestionOptions::default()
            .with_starting_block(self.starting_block.unwrap_or(0))
            .with_segment_options(self.segment.to_segment_options())
    }
}
