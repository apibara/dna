use std::{path::PathBuf, process::ExitCode, time::Instant};

use apibara_dna_common::{
    error::{DnaError, ReportExt, Result},
    ingestion::IngestionServer,
    segment::{SegmentArgs, SnapshotReader},
    storage::{AzureStorageBackendBuilder, LocalStorageBackend, StorageBackend},
};
use apibara_dna_evm::{
    ingestion::{ChainTracker, Ingestor, IngestorOptions, RpcProviderService},
    segment::{
        store, BlockHeaderSegmentReader, LogSegmentReader, SegmentGroupExt, SegmentGroupReader,
        TransactionSegmentReader,
    },
    server::DnaServer,
};
use apibara_observability::init_opentelemetry;
use clap::{Args, Parser, Subcommand};
use error_stack::ResultExt;
use futures_util::Future;
use roaring::RoaringBitmap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

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
    StartServer(StartServerArgs),
    Inspect(InspectArgs),
}

/// Start ingesting data from Ethereum.
///
/// If a snapshot is already present, it will be used to resume ingestion.
#[derive(Args, Debug)]
struct StartIngestionArgs {
    /// Start ingesting data from this block, replacing any existing snapshot.
    #[arg(long, env)]
    pub starting_block: Option<u64>,
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    #[clap(flatten)]
    pub segment: SegmentArgs,
    #[clap(flatten)]
    pub ingestor: IngestorArgs,
    #[clap(flatten)]
    pub rpc: RpcArgs,
}

/// Start the DNA server.
#[derive(Args, Debug)]
struct StartServerArgs {
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    /// Ingestion server URL.
    #[clap(long, env)]
    pub ingestion_server: String,
}

#[derive(Args, Debug, Clone)]
#[group(required = true, multiple = false)]
struct StorageArgs {
    #[arg(long, env)]
    local_dir: Option<PathBuf>,
    #[arg(long, env)]
    azure_container: Option<String>,
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

#[derive(Args, Debug, Clone)]
struct IngestorArgs {
    /// Fetch transactions for each block in a single call.
    #[arg(long, env, default_value = "true")]
    pub rpc_get_block_by_number_with_transactions: bool,
    /// Use `eth_getBlockReceipts` instead of `eth_getTransactionReceipt`.
    #[arg(long, env, default_value = "false")]
    pub rpc_get_block_receipts_by_number: bool,
}

#[derive(Args, Debug)]
struct InspectArgs {
    #[clap(flatten)]
    storage: StorageArgs,
    /// Print found data.
    #[arg(long, env)]
    pub log: bool,
    #[clap(flatten)]
    pub logs: InspectLogsArgs,
    #[clap(flatten)]
    pub transaction: InspectTransactionArgs,
    #[clap(flatten)]
    pub header: InspectHeadersArgs,
}

#[derive(Args, Debug)]
struct InspectHeadersArgs {
    #[arg(long, env, default_value = "false")]
    pub header: bool,
}

#[derive(Args, Debug)]
struct InspectTransactionArgs {
    #[arg(long, env)]
    pub to_address: Option<String>,
}

#[derive(Args, Debug)]
struct InspectLogsArgs {
    /// Address to inspect.
    #[arg(long, env)]
    pub address: Option<String>,
    /// Topic to inspect.
    #[arg(long, env)]
    pub topic: Option<String>,
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
        Command::StartServer(args) => run_server(args).await,
        Command::Inspect(args) => run_inspect(args).await,
    }
}

async fn run_ingestion(args: StartIngestionArgs) -> Result<()> {
    info!(from_block = ?args.starting_block, "Starting EVM ingestion");
    let output = &args.storage;
    if let Some(local_dir) = &output.local_dir {
        let storage = LocalStorageBackend::new(local_dir);
        run_ingestion_with_storage(args, storage).await
    } else if let Some(azure_container) = &output.azure_container {
        let storage = AzureStorageBackendBuilder::from_env()
            .with_container_name(azure_container)
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to build Azure storage backend")
            .attach_printable("hint: have you set the AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables?")?;
        run_ingestion_with_storage(args, storage).await
    } else {
        Err(DnaError::Configuration)
            .attach_printable("no storage backend configured")
            .change_context(DnaError::Fatal)
    }
}

async fn run_ingestion_with_storage<S>(args: StartIngestionArgs, storage: S) -> Result<()>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
{
    let ct = CancellationToken::new();

    let provider = RpcProviderService::new(args.rpc.rpc_url)?
        .with_rate_limit(args.rpc.rpc_rate_limit as u32)
        .with_concurrency(args.rpc.rpc_concurrency)
        .start(ct.clone());

    let chain_changes = ChainTracker::new(provider.clone()).start(ct.clone());

    let ingestor_options = IngestorOptions::default()
        .with_segment_options(args.segment.to_segment_options())
        .with_starting_block(args.starting_block.unwrap_or_default())
        .with_get_block_by_number_with_transactions(
            args.ingestor.rpc_get_block_by_number_with_transactions,
        )
        .with_get_block_receipts_by_number(args.ingestor.rpc_get_block_receipts_by_number);

    let ingestion_stream = Ingestor::new(provider, storage, chain_changes)
        .with_options(ingestor_options)
        .start(ct.clone());

    let server = IngestionServer::new(ingestion_stream);

    let address = "0.0.0.0:7007".parse().expect("parse address");

    server.start(address, ct).await?;

    Ok(())
}

async fn run_server(args: StartServerArgs) -> Result<()> {
    info!("Starting DNA server");
    let output = &args.storage;
    if let Some(local_dir) = &output.local_dir {
        let storage = LocalStorageBackend::new(local_dir);
        run_server_with_storage(args, storage).await
    } else if let Some(azure_container) = &output.azure_container {
        let storage = AzureStorageBackendBuilder::from_env()
            .with_container_name(azure_container)
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to build Azure storage backend")
            .attach_printable("hint: have you set the AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables?")?;
        run_server_with_storage(args, storage).await
    } else {
        Err(DnaError::Configuration)
            .attach_printable("no storage backend configured")
            .change_context(DnaError::Fatal)
    }
}

async fn run_server_with_storage<S>(args: StartServerArgs, storage: S) -> Result<()>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    let ct = CancellationToken::new();

    let server = DnaServer::new(args.ingestion_server, storage);
    let address = "0.0.0.0:7070".parse().expect("parse address");

    server.start(address, ct).await?;

    Ok(())
}

async fn run_inspect(args: InspectArgs) -> Result<()> {
    let storage = &args.storage;
    if let Some(local_dir) = &storage.local_dir {
        let storage = LocalStorageBackend::new(local_dir);
        run_inspect_with_storage(args, storage).await
    } else if let Some(azure_container) = &storage.azure_container {
        let storage = AzureStorageBackendBuilder::from_env()
            .with_container_name(azure_container)
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to build Azure storage backend")
            .attach_printable("hint: have you set the AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables?")?;
        let _ = tokio::spawn(run_inspect_with_storage(args, storage)).await;
        Ok(())
    } else {
        Err(DnaError::Configuration)
            .attach_printable("no storage backend configured")
            .change_context(DnaError::Fatal)
    }
}

fn run_inspect_with_storage<S>(
    args: InspectArgs,
    storage: S,
) -> impl Future<Output = Result<()>> + Send
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
{
    async move {
        let mut snapshot_reader = SnapshotReader::new(storage.clone());
        let snapshot = snapshot_reader.snapshot_state().await?;

        let segment_options = snapshot.segment_options;
        let starting_block_number =
            segment_options.segment_group_start(snapshot.first_block_number);
        let ending_block_number = starting_block_number
            + snapshot.group_count as u64 * segment_options.segment_group_blocks() as u64;

        let mut segment_group_reader =
            SegmentGroupReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);
        let mut header_segment_reader = BlockHeaderSegmentReader::new(
            storage.clone(),
            segment_options.clone(),
            1024 * 1024 * 1024,
        );
        let mut log_segment_reader =
            LogSegmentReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);
        let mut transaction_segment_reader = TransactionSegmentReader::new(
            storage.clone(),
            segment_options.clone(),
            1024 * 1024 * 1024,
        );

        let address_filter = if let Some(address) = args.logs.address {
            info!(address, "Filter by log address");
            let address = store::Address::from_hex(&address)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to parse address")?;
            Some(address)
        } else {
            None
        };

        let transaction_to_filter = if let Some(to_address) = args.transaction.to_address {
            info!(to_address, "Filter by transaction to address");
            let address = store::Address::from_hex(&to_address)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to parse address")?;
            Some(address)
        } else {
            None
        };

        /*
        let topic_filter = if let Some(topic) = args.logs.topic {
            todo!()
        } else {
            None
        };
        */

        let mut current_block_number = starting_block_number;

        let start_time = Instant::now();

        let mut block_bitmap = RoaringBitmap::new();
        let mut event_count = 0;
        let mut transaction_count = 0;
        let mut withdrawal_count = 0;
        let mut segment_read_count = 0;
        while current_block_number < ending_block_number {
            let current_segment_group_start =
                segment_options.segment_group_start(current_block_number);
            debug!(current_segment_group_start, "reading new segment group");
            let segment_group = segment_group_reader
                .read(current_segment_group_start)
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read segment group")?;

            assert_eq!(
                segment_group.first_block_number(),
                current_segment_group_start
            );

            let segment_group_blocks = segment_options.segment_group_blocks();
            let segment_group_end = current_segment_group_start + segment_group_blocks;

            block_bitmap.clear();
            // TODO: use bitmap for transactions.
            if args.header.header || transaction_to_filter.is_some() {
                block_bitmap
                    .insert_range(current_segment_group_start as u32..segment_group_end as u32);
            } else if let Some(address) = &address_filter {
                let address_bitmap = segment_group
                    .get_log_by_address(address)
                    .unwrap_or_default();
                debug!(address = %address, address_bitmap = ?address_bitmap, "read address bitmap");
                block_bitmap |= address_bitmap;
            }

            // Skip as many segments in the group as possible.
            if let Some(starting_block) = block_bitmap.min() {
                current_block_number = starting_block as u64;
            } else {
                debug!(segment_group_end, "no blocks to read. skip ahead");
                current_block_number = segment_group_end;
                continue;
            }

            let mut current_segment_start = segment_options.segment_start(current_block_number);
            debug!(current_segment_start, "reading starting segment");

            let mut header_segment = if args.header.header {
                Some(
                    header_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read header segment")?,
                )
            } else {
                None
            };

            let mut log_segment = if address_filter.is_some() {
                Some(
                    log_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to log segment")?,
                )
            } else {
                None
            };

            let mut transaction_segment = if transaction_to_filter.is_some() {
                Some(
                    transaction_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read transaction segment")?,
                )
            } else {
                None
            };

            for block_number in block_bitmap.iter() {
                if current_segment_start < segment_options.segment_start(block_number as u64) {
                    current_segment_start = segment_options.segment_start(block_number as u64);
                    segment_read_count += 1;
                    debug!(current_segment_start, "reading new segment");
                    if header_segment.is_some() {
                        header_segment = Some(
                            header_segment_reader
                                .read(current_segment_start)
                                .await
                                .change_context(DnaError::Fatal)
                                .attach_printable("failed to read header segment")?,
                        )
                    };

                    if log_segment.is_some() {
                        log_segment = Some(
                            log_segment_reader
                                .read(current_segment_start)
                                .await
                                .change_context(DnaError::Fatal)
                                .attach_printable("failed to read log segment")?,
                        )
                    };

                    if transaction_segment.is_some() {
                        transaction_segment = Some(
                            transaction_segment_reader
                                .read(current_segment_start)
                                .await
                                .change_context(DnaError::Fatal)
                                .attach_printable("failed to read transaction segment")?,
                        )
                    };
                }

                debug!(block_number, "inspect block");

                if let Some(log_segment) = log_segment.as_ref() {
                    let target_address = address_filter.as_ref().unwrap();

                    let index = block_number - log_segment.first_block_number() as u32;
                    let block_logs = log_segment.blocks().unwrap_or_default().get(index as usize);

                    for log in block_logs.logs().unwrap_or_default() {
                        let address = log.address().expect("address is missing");
                        if address != target_address {
                            continue;
                        }

                        let _topics = log.topics().unwrap_or_default();
                        let _data = log.data().unwrap_or_default();

                        let log_index = log.log_index();
                        let transaction_index = log.transaction_index();
                        let transaction_hash = log.transaction_hash().unwrap();

                        if args.log {
                            info!(
                                block_number,
                                transaction_index,
                                log_index,
                                transaction_hash = %transaction_hash.as_hex(),
                                "    log"
                            );
                        }

                        event_count += 1;
                    }
                }
                if let Some(transaction_segment) = transaction_segment.as_ref() {
                    let target_address = transaction_to_filter.as_ref().unwrap();

                    let index = block_number - transaction_segment.first_block_number() as u32;
                    let block_transactions = transaction_segment
                        .blocks()
                        .unwrap_or_default()
                        .get(index as usize);
                    for transaction in block_transactions.transactions().unwrap_or_default() {
                        let Some(to) = transaction.to() else {
                            continue;
                        };

                        if to == target_address {
                            let transaction_hash = transaction.hash().unwrap();
                            let calldata = transaction.input().unwrap_or_default();
                            if args.log {
                                info!(
                                    block_number,
                                    transaction_hash = %transaction_hash.as_hex(),
                                    calldata_len = calldata.len(),
                                    "    transaction"
                                );
                            }

                            transaction_count += 1;
                        }
                    }
                }

                if let Some(header_segment) = header_segment.as_ref() {
                    let index = block_number - header_segment.first_block_number() as u32;
                    let header = header_segment
                        .headers()
                        .unwrap_or_default()
                        .get(index as usize);

                    let miner = header.miner().expect("miner is missing");

                    if args.log {
                        info!(
                            number = header.number(),
                            miner = %miner.as_hex(),
                            "block"
                        );
                    }

                    for withdrawal in header.withdrawals().unwrap_or_default() {
                        withdrawal_count += 1;
                        let amount = withdrawal.amount().unwrap();
                        if args.log {
                            info!(
                                index = withdrawal.index(),
                                validator_index = withdrawal.validator_index(),
                                address = %withdrawal.address().unwrap().as_hex(),
                                amount = ?amount,
                                "    withdrawal"
                            );
                        }
                    }
                }
            }

            current_block_number = segment_group_end;
        }

        let elapsed = start_time.elapsed();

        let block_count = current_block_number - starting_block_number;
        let block_sec = block_count as f64 / elapsed.as_secs_f64();

        info!(
            elapsed = ?elapsed,
            block_count,
            block_sec = format!("{block_sec:.0}"),
            "block count"
        );

        let event_sec = event_count as f64 / elapsed.as_secs_f64();

        info!(
            elapsed = ?elapsed,
            event_count,
            event_sec = format!("{event_sec:.0}"),
            "event count"
        );

        let withdrawal_sec = withdrawal_count as f64 / elapsed.as_secs_f64();

        info!(
            elapsed = ?elapsed,
            withdrawal_count,
            withdrawal_sec = format!("{withdrawal_sec:.0}"),
            "withdrawal count"
        );

        let transaction_sec = transaction_count as f64 / elapsed.as_secs_f64();

        info!(
            elapsed = ?elapsed,
            transaction_count,
            transaction_sec = format!("{transaction_sec:.0}"),
            "transaction count"
        );

        let segment_read_sec = segment_read_count as f64 / elapsed.as_secs_f64();

        info!(
            elapsed = ?elapsed,
            segment_read_count,
            segment_read_sec = format!("{segment_read_sec:.0}"),
            "segment read count"
        );

        Ok(())
    }
}
