use std::{path::PathBuf, process::ExitCode};

use apibara_dna_common::error::{DnaError, ReportExt, Result};
use apibara_dna_starknet::{
    ingestion::FinalizedBlockIngestion, provider::RpcProvider, segment::BlockSegmentBuilder,
};
use apibara_observability::init_opentelemetry;
use clap::{Args, Parser, Subcommand};
use error_stack::ResultExt;
use starknet::providers::Url;
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
    Ingest(IngestArgs),
}

/// Ingest blocks from Starknet and generate segment files.
#[derive(Args, Debug)]
struct IngestArgs {
    /// Starting block.
    #[arg(long, env)]
    pub from_block: u64,
    /// Number of blocks to ingest.
    #[arg(long, env)]
    pub num_blocks: u64,
    /// Where to store the segment files.
    #[arg(long, env)]
    pub output_dir: PathBuf,
    /// Starknet RPC URL.
    #[arg(long, env)]
    pub rpc_url: String,
    /// RPC rate limit, in requests per second.
    #[arg(long, env, default_value = "512")]
    pub rpc_rate_limit: u32,
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
        Command::Ingest(args) => run_ingest(args).await,
    }
}

async fn run_ingest(args: IngestArgs) -> Result<()> {
    info!(
        from_block = args.from_block,
        num_blocks = args.num_blocks,
        output_dir = ?args.output_dir,
        rpc_url = args.rpc_url,
        rpc_rate_limit = args.rpc_rate_limit,
        "Starting Starknet ingestion"
    );

    let rpc_url = Url::parse(&args.rpc_url)
        .change_context(DnaError::Configuration)
        .attach_printable("failed to parse RPC url")?;
    let provider = RpcProvider::new(rpc_url, args.rpc_rate_limit);
    let ingestion = FinalizedBlockIngestion::new(provider);

    let mut builder = BlockSegmentBuilder::new();

    for block_number in args.from_block..args.from_block + args.num_blocks {
        info!(block_number = block_number, "Ingesting block");
        ingestion
            .ingest_block_by_number(&mut builder, block_number)
            .await?;
    }

    builder.flush().await?;

    Ok(())
}
