use apibara_dna_common::{
    error::Result, ingestion::IngestionServer, segment::SegmentArgs, storage::StorageBackend,
};
use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ingestion::{ChainTracker, IngestionOptions, IngestionService, RpcIngestionOptions};

use super::common::{CacheArgs, RpcArgs, StorageArgs};

/// Start ingesting data from Ethereum.
///
/// If a snapshot is already present, it will be used to resume ingestion.
#[derive(Args, Debug)]
pub struct StartIngestionArgs {
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
    pub rpc_ingestion: RpcIngestionArgs,
    #[clap(flatten)]
    pub segment: SegmentArgs,
}

#[derive(Args, Debug, Clone)]
pub struct RpcIngestionArgs {
    /// Fetch transactions for each block in a single call.
    #[arg(long, env, default_value = "true")]
    pub rpc_get_block_by_number_with_transactions: bool,
    /// Use `eth_getBlockReceipts` instead of `eth_getTransactionReceipt`.
    #[arg(long, env, default_value = "false")]
    pub rpc_get_block_receipts_by_number: bool,
}

pub async fn run_ingestion(args: StartIngestionArgs) -> Result<()> {
    info!("Starting EVM ingestion");
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
    // At a high level, the ingestion process is as follows:
    //
    // 1. Generate a stream of chain changes. This stream will be the single source of truth for
    //   the current chain's head and finalized blocks.
    // 2. Ingest blocks from the chain. Blocks that have been finalized are immutable and can be
    //   ingested without any additional checks. Non-finalized blocks are ingested checking that
    //   they belong to the current chain (as defined by the chain changes stream).
    // 3. Run the ingestion server, which is used by the data server to keep track of ingested data.
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

    let address = "0.0.0.0:7001".parse().expect("parse address");
    IngestionServer::new(local_cache_storage, ingestion_stream)
        .start(address, ct)
        .await
}

impl IngestionArgs {
    pub fn to_ingestion_options(&self) -> IngestionOptions {
        IngestionOptions::default()
            .with_starting_block(self.starting_block.unwrap_or(0))
            .with_segment_options(self.segment.to_segment_options())
            .with_rpc_ingestion(self.rpc_ingestion.to_rpc_ingestion_options())
    }
}

impl RpcIngestionArgs {
    pub fn to_rpc_ingestion_options(&self) -> RpcIngestionOptions {
        RpcIngestionOptions::default()
            .with_get_block_by_number_with_transactions(
                self.rpc_get_block_by_number_with_transactions,
            )
            .with_get_block_receipts_by_number(self.rpc_get_block_receipts_by_number)
    }
}
