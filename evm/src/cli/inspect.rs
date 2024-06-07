use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::Snapshot,
    storage::{AppStorageBackend, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use roaring::RoaringBitmap;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

use crate::segment::{
    BlockHeaderSegmentReader, LogSegmentReader, ReceiptSegmentReader, SegmentGroupReader,
    TransactionSegmentReader,
};

/// Inspect ingested data.
#[derive(Args, Debug)]
pub struct InspectArgs {
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    #[clap(flatten)]
    pub entity: EntityArgs,
    #[clap(long, action)]
    pub verbose: bool,
}

#[derive(Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct EntityArgs {
    /// Inspect a segment group.
    #[arg(long)]
    pub group: Option<u64>,
    /// Inspect an header segment.
    #[arg(long)]
    pub header: Option<u64>,
    /// Inspect a log segment.
    #[arg(long)]
    pub log: Option<u64>,
    /// Inspect a receipt segment.
    #[arg(long)]
    pub receipt: Option<u64>,
    /// Inspect a transaction segment.
    #[arg(long)]
    pub transaction: Option<u64>,
}

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

pub async fn run_inspect(args: InspectArgs) -> Result<()> {
    let mut storage = args.storage.to_app_storage_backend()?;

    let snapshot = load_snapshot(&mut storage).await?;
    info!(revision = snapshot.revision, "loaded snapshot from storage");

    if let Some(block_number) = args.entity.group {
        return inspect_group(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.header {
        return inspect_header(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.log {
        return inspect_log(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.receipt {
        return inspect_receipt(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.transaction {
        return inspect_transaction(storage, snapshot, block_number, args.verbose).await;
    }

    Ok(())
}

async fn inspect_group(
    storage: AppStorageBackend,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
) -> Result<()> {
    let mut segment_group_reader = SegmentGroupReader::new(
        storage.clone(),
        snapshot.segment_options.clone(),
        DEFAULT_BUFFER_SIZE,
    );

    let segment_group_start = snapshot.segment_options.segment_group_start(block_number);
    let segment_group_end = segment_group_start + snapshot.segment_options.segment_group_blocks();
    let segment_group = segment_group_reader.read(segment_group_start).await?;

    info!(
        first_block_number = segment_group.first_block_number(),
        "read segment group"
    );

    let log_by_address = segment_group.log_by_address().unwrap_or_default();
    info!(size = log_by_address.len(), "inspect log by address index");
    for item in log_by_address.iter() {
        let Some(address) = item.key() else {
            warn!("log by address key is missing");
            continue;
        };
        let Some(bitmap) = item.bitmap() else {
            warn!("log by address bitmap is missing");
            continue;
        };

        let bitmap = RoaringBitmap::deserialize_from(bitmap.bytes())
            .change_context(DnaError::Fatal)
            .attach_printable("failed to deserialize log by address bitmap")?;

        if let Some(min) = bitmap.min() {
            if min < segment_group_start as u32 {
                warn!("log by address contains out-of-range block number (min)");
            }
        }

        if let Some(max) = bitmap.max() {
            if max > segment_group_end as u32 {
                warn!("log by address contains out-of-range block number (max)");
            }
        }

        if verbose {
            info!("log by address: {} {:?}", address, bitmap);
        }
    }

    let log_by_topic = segment_group.log_by_topic().unwrap_or_default();
    info!(size = log_by_topic.len(), "inspect log by topic index");
    for item in log_by_topic.iter() {
        let Some(topic) = item.key() else {
            warn!("log by topic key is missing");
            continue;
        };
        let Some(bitmap) = item.bitmap() else {
            warn!("log by topic bitmap is missing");
            continue;
        };

        let bitmap = RoaringBitmap::deserialize_from(bitmap.bytes())
            .change_context(DnaError::Fatal)
            .attach_printable("failed to deserialize log by topic bitmap")?;

        if let Some(min) = bitmap.min() {
            if min < segment_group_start as u32 {
                warn!("log by topic contains out-of-range block number (min)");
            }
        }

        if let Some(max) = bitmap.max() {
            if max > segment_group_end as u32 {
                warn!("log by topic contains out-of-range block number (max)");
            }
        }

        if verbose {
            info!("log by topic: {} {:?}", topic, bitmap);
        }
    }

    Ok(())
}

async fn inspect_header(
    storage: AppStorageBackend,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
) -> Result<()> {
    let mut segment_reader = BlockHeaderSegmentReader::new(
        storage.clone(),
        snapshot.segment_options.clone(),
        DEFAULT_BUFFER_SIZE,
    );

    let segment_start = snapshot.segment_options.segment_start(block_number);
    let header_segment = segment_reader.read(segment_start).await?;

    info!(start = segment_start, "read header segment");

    if segment_start != header_segment.first_block_number() {
        warn!(
            segment_start,
            first_block_number = header_segment.first_block_number(),
            "header segment first block number mismatch"
        );
    }

    let headers = header_segment.headers().unwrap_or_default();
    if headers.len() != snapshot.segment_options.segment_size {
        warn!(
            size = headers.len(),
            expected_size = snapshot.segment_options.segment_size,
            "header segment size mismatch"
        );
    }

    info!(size = headers.len(), "inspect headers");
    for (header, expected_block_number) in headers.iter().zip(segment_start..) {
        if header.number() != expected_block_number {
            warn!(
                header_number = header.number(),
                expected_block_number, "header number mismatch"
            );
        }

        let Some(hash) = header.hash() else {
            warn!("header hash is missing");
            continue;
        };

        if verbose {
            info!(
                "header: number={} hash={} ts={}",
                header.number(),
                hash,
                header.timestamp()
            );
        }
    }

    Ok(())
}

async fn inspect_transaction(
    storage: AppStorageBackend,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
) -> Result<()> {
    let mut segment_reader = TransactionSegmentReader::new(
        storage.clone(),
        snapshot.segment_options.clone(),
        DEFAULT_BUFFER_SIZE,
    );

    let segment_start = snapshot.segment_options.segment_start(block_number);
    let transaction_segment = segment_reader.read(segment_start).await?;

    info!(start = segment_start, "read transaction segment");

    let blocks = transaction_segment.blocks().unwrap_or_default();
    if blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "blocks segment size mismatch"
        );
    }

    info!(size = blocks.len(), "inspect transaction blocks");
    for (block, expected_block_number) in blocks.iter().zip(segment_start..) {
        if block.block_number() != expected_block_number {
            warn!(
                block_number = block.block_number(),
                expected_block_number, "block number mismatch"
            );
        }

        let transactions = block.transactions().unwrap_or_default();
        info!(
            block_number = block.block_number(),
            size = transactions.len(),
            "inspect transactions"
        );
        for (expected_index, transaction) in transactions.iter().enumerate() {
            if expected_index != transaction.transaction_index() as usize {
                warn!(
                    transaction_index = transaction.transaction_index(),
                    expected_index, "transaction index mismatch"
                );
            }
            let Some(hash) = transaction.hash() else {
                warn!("transaction hash is missing");
                continue;
            };

            if verbose {
                info!(
                    "transaction: index={} hash={}",
                    transaction.transaction_index(),
                    hash,
                );
            }
        }
    }

    Ok(())
}

async fn inspect_receipt(
    storage: AppStorageBackend,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
) -> Result<()> {
    let mut segment_reader = ReceiptSegmentReader::new(
        storage.clone(),
        snapshot.segment_options.clone(),
        DEFAULT_BUFFER_SIZE,
    );

    let segment_start = snapshot.segment_options.segment_start(block_number);
    let receipt_segment = segment_reader.read(segment_start).await?;

    info!(start = segment_start, "read receipt segment");

    let blocks = receipt_segment.blocks().unwrap_or_default();
    if blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "blocks segment size mismatch"
        );
    }

    info!(size = blocks.len(), "inspect receipt blocks");
    for (block, expected_block_number) in blocks.iter().zip(segment_start..) {
        if block.block_number() != expected_block_number {
            warn!(
                block_number = block.block_number(),
                expected_block_number, "block number mismatch"
            );
        }

        let receipts = block.receipts().unwrap_or_default();
        info!(
            block_number = block.block_number(),
            size = receipts.len(),
            "inspect receipts"
        );
        for (expected_index, receipt) in receipts.iter().enumerate() {
            if expected_index != receipt.transaction_index() as usize {
                warn!(
                    transaction_index = receipt.transaction_index(),
                    expected_index, "transaction index mismatch"
                );
            }
            let Some(hash) = receipt.transaction_hash() else {
                warn!("transaction hash is missing");
                continue;
            };

            if verbose {
                info!(
                    "receipt: index={} hash={}",
                    receipt.transaction_index(),
                    hash,
                );
            }
        }
    }

    Ok(())
}

async fn inspect_log(
    storage: AppStorageBackend,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
) -> Result<()> {
    let mut segment_reader = LogSegmentReader::new(
        storage.clone(),
        snapshot.segment_options.clone(),
        DEFAULT_BUFFER_SIZE,
    );

    let segment_start = snapshot.segment_options.segment_start(block_number);
    let log_segment = segment_reader.read(segment_start).await?;

    info!(start = segment_start, "read log segment");

    let blocks = log_segment.blocks().unwrap_or_default();
    if blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "blocks segment size mismatch"
        );
    }

    info!(size = blocks.len(), "inspect log blocks");
    for (block, expected_block_number) in blocks.iter().zip(segment_start..) {
        if block.block_number() != expected_block_number {
            warn!(
                block_number = block.block_number(),
                expected_block_number, "block number mismatch"
            );
        }

        let logs = block.logs().unwrap_or_default();
        info!(
            block_number = block.block_number(),
            size = logs.len(),
            "inspect logs"
        );

        let mut prev_transaction_index = 0;
        for (expected_index, log) in logs.iter().enumerate() {
            let transaction_index = log.transaction_index();

            if transaction_index < prev_transaction_index {
                warn!(
                    transaction_index,
                    prev_transaction_index, "log transaction index is out of order"
                );
            }
            if expected_index != log.log_index() as usize {
                warn!(
                    log_index = log.log_index(),
                    expected_index, "log index mismatch"
                );
            }

            let Some(address) = log.address() else {
                warn!("log address is missing");
                continue;
            };

            let Some(topics) = log.topics() else {
                warn!("log topics are missing");
                continue;
            };

            if log.data().is_none() {
                warn!("log data is missing");
            };

            let Some(transaction_hash) = log.transaction_hash() else {
                warn!("log transaction hash is missing");
                continue;
            };

            if verbose {
                info!(
                    "log: tx_index={} log_index={} address={} topics_size={} tx_hash={}",
                    transaction_index,
                    log.log_index(),
                    address,
                    topics.len(),
                    transaction_hash,
                );
            }

            prev_transaction_index = transaction_index;
        }
    }

    Ok(())
}

async fn load_snapshot(storage: &mut AppStorageBackend) -> Result<Snapshot> {
    if !storage.exists("", "snapshot").await? {
        return Err(DnaError::Fatal)
            .attach_printable("snapshot file not found")
            .attach_printable("hint: does the storage argument point to the correct location?");
    }

    let mut reader = storage.get("", "snapshot").await?;
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
}
