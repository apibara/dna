use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::IngestionServer,
    segment::SegmentArgs,
    storage::{CacheArgs, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{BeaconApiProvider, BlockId, ChainTracker};

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

    let provider = BeaconApiProvider::new(&args.rpc.rpc_url);

    let mut chain_changes = ChainTracker::new(provider.clone()).start(ct.clone());

    while let Some(chain_change) = chain_changes.next().await {
        println!("{:?}", chain_change);
    }

    todo!();
}
