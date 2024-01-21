use std::{path::PathBuf, process::ExitCode, time::Instant};

use apibara_dna_common::{
    error::{DnaError, ReportExt, Result},
    segment::{SegmentExt, SegmentOptions},
    storage::{FormattedSize, LocalStorageBackend, StorageBackend, StorageReader},
};
use apibara_dna_starknet::{
    ingestion::RpcBlockDownloader,
    provider::RpcProvider,
    segment::{store, SegmentBuilder, SegmentGroupBuilder, Snapshot},
};
use apibara_observability::init_opentelemetry;
use bytes::BytesMut;
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
    Inspect(InspectArgs),
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

/// Inspect data generated by `ingest`.
#[derive(Args, Debug)]
struct InspectArgs {
    /// Root directory of the segment files.
    #[arg(long, env)]
    pub root_dir: PathBuf,
    /// Filter events by contract address.
    #[arg(long, env)]
    pub filter_event_address: Option<String>,
    /// Log to stdout.
    #[arg(long, env, default_value = "false")]
    pub log: bool,
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
        Command::Inspect(args) => run_inspect(args).await,
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

async fn run_inspect(args: InspectArgs) -> Result<()> {
    info!(root_dir = ?args.root_dir, "Inspecting Starknet data");

    let mut storage = LocalStorageBackend::new(args.root_dir);

    let mut reader = storage.reader("").await?;

    let mut bytes = BytesMut::zeroed(200_000_000);
    info!(capacity = %FormattedSize(bytes.len()), "prepare buffer with capacity");
    let snapshot_size = reader.copy_to_slice("snapshot", &mut bytes).await?;
    let snapshot = flatbuffers::root::<store::Snapshot>(&bytes[..snapshot_size])
        .change_context(DnaError::Fatal)
        .attach_printable("failed to parse snapshot")?;

    let segment_options = SegmentOptions {
        segment_size: snapshot.segment_size() as usize,
        group_size: snapshot.group_size() as usize,
        target_num_digits: 9,
    };

    info!(
        first_block_number = snapshot.first_block_number(),
        segment_size = snapshot.segment_size(),
        group_size = snapshot.group_size(),
        group_count = snapshot.group_count(),
        "read snapshot"
    );

    let end_block = snapshot.first_block_number()
        + (snapshot.segment_size() as u64
            * snapshot.group_size() as u64
            * snapshot.group_count() as u64);

    let mut segment_reader = storage.reader("segment").await?;

    let filter_event_address = if let Some(filter_event_address) = args.filter_event_address {
        Some(store::FieldElement::from_hex(&filter_event_address)?)
    } else {
        None
    };

    let mut events_count = 0;
    let mut matched_events_count = 0;
    let mut blocks_count = 0;
    let start_time = Instant::now();
    let mut current_block = snapshot.first_block_number();
    while current_block < end_block {
        let segment_name = current_block.format_segment_name(&segment_options);
        let segment_size = segment_reader
            .copy_to_slice(&format!("{segment_name}/event.segment"), &mut bytes)
            .await?;

        if args.log {
            info!(
                segment_name = %segment_name,
                segment_size = %FormattedSize(segment_size),
                "read segment"
            );
        }

        let segment = flatbuffers::root::<store::EventSegment>(&bytes[..segment_size])
            .change_context(DnaError::Fatal)
            .attach_printable("failed to parse segment")?;

        if let Some(blocks) = segment.blocks() {
            current_block += blocks.len() as u64;
            blocks_count += blocks.len();
            for block in blocks.iter() {
                let block_events = block.events().unwrap_or_default();
                let block_number = block.block_number();
                for event in block_events.iter() {
                    events_count += 1;

                    let from_address = event.from_address().unwrap();
                    if let Some(filter) = filter_event_address.as_ref() {
                        if from_address != filter {
                            continue;
                        }
                        let _keys = event.keys().unwrap();
                        let _data = event.data().unwrap();
                        let transaction_hash = event.transaction_hash().unwrap();
                        let transaction_index = event.transaction_index();
                        matched_events_count += 1;

                        if args.log {
                            info!(
                                block_number = block_number,
                                transaction_index = transaction_index,
                                transaction_hash = %transaction_hash,
                                "event"
                            );
                        }
                    }
                }
            }
        }
    }

    let elapsed = start_time.elapsed();
    let blocks_sec = blocks_count as f64 / elapsed.as_secs_f64();
    let matches_sec = matched_events_count as f64 / elapsed.as_secs_f64();
    let events_sec = events_count as f64 / elapsed.as_secs_f64();
    let matches_ratio = matched_events_count as f64 / events_count as f64;
    let events_per_block = matched_events_count as f64 / blocks_count as f64;

    info!(
        elapsed = ?elapsed,
        blocks_count = blocks_count,
        blocks_sec = format!("{blocks_sec:.0}"),
        events_count = events_count,
        events_sec = format!("{events_sec:.0}"),
        matches_count = matched_events_count,
        matches_sec = format!("{matches_sec:.0}"),
        matches_ratio = format!("{:.0}%", matches_ratio * 100.0),
        events_per_block = format!("{events_per_block:.0}"),
        "finished reading events"
    );

    Ok(())
}
