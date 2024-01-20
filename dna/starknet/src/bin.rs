use std::{path::PathBuf, process::ExitCode};

use apibara_dna_common::{
    error::{DnaError, ReportExt, Result},
    segment::{SegmentExt, SegmentOptions},
    storage::LocalStorageBackend,
};
use apibara_dna_starknet::{
    ingestion::RpcBlockDownloader,
    provider::RpcProvider,
    segment::{SegmentBuilder, SegmentGroupBuilder, Snapshot},
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
    /// Size of each segment.
    #[arg(long, env)]
    pub segment_size: usize,
    /// Size of each segment group.
    #[arg(long, env)]
    pub segment_group_size: usize,
    /// Number of segment groups to ingest.
    #[arg(long, env)]
    pub segment_group_count: usize,
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
        segment_size = args.segment_size,
        segment_group_size = args.segment_group_size,
        segment_group_count = args.segment_group_count,
        output_dir = ?args.output_dir,
        rpc_url = args.rpc_url,
        rpc_rate_limit = args.rpc_rate_limit,
        "Starting Starknet ingestion"
    );

    let rpc_url = Url::parse(&args.rpc_url)
        .change_context(DnaError::Configuration)
        .attach_printable("failed to parse RPC url")?;
    let provider = RpcProvider::new(rpc_url, args.rpc_rate_limit);

    let block_downloader = RpcBlockDownloader::new(provider);

    let storage = LocalStorageBackend::new(args.output_dir);

    let segment_options = SegmentOptions {
        segment_size: args.segment_size,
        group_size: args.segment_group_size,
        target_num_digits: 9,
    };

    let mut current_block = args.from_block.segment_start(&segment_options);

    let mut segment_builder = SegmentBuilder::new(storage.clone(), segment_options.clone());
    let mut segment_group_builder =
        SegmentGroupBuilder::new(storage.clone(), segment_options.clone());
    let mut snapshot = Snapshot::new(args.from_block, storage, segment_options);

    loop {
        if snapshot.group_count() >= args.segment_group_count {
            break;
        }

        let block = block_downloader
            .download_block_by_number(current_block)
            .await?;
        current_block += 1;

        let segment_event = segment_builder.ingest_block(current_block, block).await?;

        let segment_group_event = segment_group_builder
            .handle_segment_event(segment_event)
            .await?;

        snapshot
            .handle_segment_group_event(segment_group_event)
            .await?;
    }

    Ok(())
}
