use std::{path::PathBuf, process::ExitCode};

use apibara_dna_common::{
    error::{DnaError, ReportExt, Result},
    segment::SegmentArgs,
    storage::LocalStorageBackend,
};
use apibara_dna_evm::ingestion::{Ingestor, RpcProviderService};
use apibara_observability::init_opentelemetry;
use clap::{Args, Parser, Subcommand};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    StartIngestion(StartIngestionArgs),
}

/// Start ingesting data from Ethereum.
///
/// If a snapshot is already present, it will be used to resume ingestion.
#[derive(Args, Debug)]
struct StartIngestionArgs {
    /// Start ingesting data from this block.
    ///
    /// Notice that if a client requests data from a block that is earlier than
    /// this block, it will error.
    #[arg(long, env, default_value = "0")]
    pub from_block: u64,
    /// Location for ingested data.
    #[arg(long, env)]
    pub data_dir: PathBuf,
    #[clap(flatten)]
    pub segment: SegmentArgs,
    #[clap(flatten)]
    pub rpc: RpcArgs,
}

#[derive(Args, Debug, Clone)]
struct RpcArgs {
    /// Ethereum RPC URL.
    #[arg(long, env)]
    pub rpc_url: String,
    /// RPC rate limit, in requests per second.
    #[arg(long, env, default_value = "1000")]
    pub rpc_rate_limit: usize,
    /// How many concurrent requests to send.
    #[arg(long, env, default_value = "100")]
    pub rpc_concurrency: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();
    run_with_args(args).await.to_exit_code()
}

async fn run_with_args(args: Cli) -> Result<()> {
    init_opentelemetry()
        .change_context(DnaError::Fatal)
        .attach_printable("failed to initialize opentelemetry")?;

    match args.subcommand {
        Command::StartIngestion(args) => run_ingestion(args).await,
    }
}

async fn run_ingestion(args: StartIngestionArgs) -> Result<()> {
    info!(from_block = %args.from_block, "Starting EVM ingestion");
    info!(data_dir = %args.data_dir.display(), "Using data directory");
    let segment_options = args.segment.to_segment_options();
    info!(segment_options = ?segment_options, "Using segment options");

    let ct = CancellationToken::new();

    let (provider, rpc_provider_fut) = RpcProviderService::new(args.rpc.rpc_url)?
        .with_rate_limit(args.rpc.rpc_rate_limit as u32)
        .with_concurrency(args.rpc.rpc_concurrency)
        .start(ct.clone());

    let storage = LocalStorageBackend::new(args.data_dir);
    let starting_block_number = segment_options.segment_group_start(args.from_block);
    let ingestor = Ingestor::new(provider, storage).with_segment_options(segment_options);

    let rpc_provider_task = tokio::spawn(rpc_provider_fut);
    let ingestion_task = tokio::spawn({
        let ct = ct.clone();
        async move { ingestor.start(starting_block_number, ct).await }
    });

    tokio::select! {
        _ = rpc_provider_task => {
            info!("rpc provider task finished");
        }
        _ = ingestion_task => {
            info!("ingestion task finished");
        }
    }

    Ok(())
}
