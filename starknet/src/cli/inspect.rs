use std::time::Instant;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::Snapshot,
    storage::{AppStorageBackend, CacheArgs, CachedStorage, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use roaring::RoaringBitmap;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

use crate::{ingestion::models, segment::store};

/// Inspect ingested data.
#[derive(Args, Debug)]
pub struct InspectArgs {
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    #[clap(flatten)]
    pub cache: CacheArgs,
    #[clap(flatten)]
    pub entity: EntityArgs,
    #[clap(long, action)]
    pub verbose: bool,
    #[clap(long, action)]
    pub parse_only: bool,
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
    /// Inspect an event segment.
    #[arg(long)]
    pub event: Option<u64>,
    /// Inspect a message segment.
    #[arg(long)]
    pub message: Option<u64>,
    /// Inspect a receipt segment.
    #[arg(long)]
    pub receipt: Option<u64>,
    /// Inspect a transaction segment.
    #[arg(long)]
    pub transaction: Option<u64>,
}

pub async fn run_inspect(args: InspectArgs) -> Result<()> {
    let storage = args
        .storage
        .to_app_storage_backend()
        .attach_printable("failed to initialize storage backend")?;
    let local_cache_storage = args.cache.to_local_storage_backend();
    let mut storage = CachedStorage::new(local_cache_storage.clone(), storage, &[]);

    let snapshot = load_snapshot(&mut storage).await?;
    info!(revision = snapshot.revision, "loaded snapshot from storage");

    let start_time = Instant::now();

    if let Some(block_number) = args.entity.group {
        inspect_group(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    } else if let Some(block_number) = args.entity.header {
        inspect_header(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    } else if let Some(block_number) = args.entity.event {
        inspect_event(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    } else if let Some(block_number) = args.entity.message {
        inspect_message(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    } else if let Some(block_number) = args.entity.receipt {
        inspect_receipt(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    } else if let Some(block_number) = args.entity.transaction {
        inspect_transaction(
            storage,
            snapshot,
            block_number,
            args.verbose,
            args.parse_only,
        )
        .await?;
    }

    let elapsed = start_time.elapsed();
    info!(elapsed = ?elapsed, "inspect completed");

    Ok(())
}

async fn inspect_group(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_group_start = snapshot.segment_options.segment_group_start(block_number);
    let segment_group_end = segment_group_start + snapshot.segment_options.segment_group_blocks();

    let segment_group_name = snapshot
        .segment_options
        .format_segment_group_name(segment_group_start);

    info!(segment_group_name, "deserializing segment group");

    let bytes = storage.mmap("group", segment_group_name).await?;

    let segment_group = rkyv::from_bytes::<store::SegmentGroup>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse segment group")?;

    if parse_only {
        return Ok(());
    }

    info!(
        event_by_address_size = segment_group.index.event_by_address.len(),
        "inspect event by address index"
    );

    for (address, bitmap) in segment_group.index.event_by_address.iter() {
        let bitmap = RoaringBitmap::deserialize_from(bitmap.0.as_slice())
            .change_context(DnaError::Fatal)
            .attach_printable("failed to deserialize event by address bitmap")?;

        if let Some(min) = bitmap.min() {
            if min < segment_group_start as u32 {
                warn!("event by address contains out-of-range block number (min)");
            }
        }

        if let Some(max) = bitmap.max() {
            if max > segment_group_end as u32 {
                warn!("event by address contains out-of-range block number (max)");
            }
        }

        if verbose {
            let address = models::FieldElement::from(address);
            info!("event by address: 0x{:x} {:?}", address, bitmap);
        }
    }

    info!(
        event_by_key_0_size = segment_group.index.event_by_key_0.len(),
        "inspect event by key[0] index"
    );

    for (key, bitmap) in segment_group.index.event_by_key_0.iter() {
        let bitmap = RoaringBitmap::deserialize_from(bitmap.0.as_slice())
            .change_context(DnaError::Fatal)
            .attach_printable("failed to deserialize event by key bitmap")?;

        if let Some(min) = bitmap.min() {
            if min < segment_group_start as u32 {
                warn!("event by key contains out-of-range block number (min)");
            }
        }

        if let Some(max) = bitmap.max() {
            if max > segment_group_end as u32 {
                warn!("event by key contains out-of-range block number (max)");
            }
        }

        if verbose {
            let key = models::FieldElement::from(key);
            info!("event by key: 0x{:x} {:?}", key, bitmap);
        }
    }

    Ok(())
}

async fn inspect_header(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_start = snapshot.segment_options.segment_start(block_number);
    let segment_name = snapshot.segment_options.format_segment_name(block_number);

    info!(start = segment_start, "deserializing header segment");

    let bytes = storage
        .mmap(format!("segment/{segment_name}"), "header")
        .await?;

    let header_segment = rkyv::from_bytes::<store::BlockHeaderSegment>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse header segment")?;

    if parse_only {
        return Ok(());
    }

    if header_segment.blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = header_segment.blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "header segment size mismatch"
        );
    }

    for (header, expected_block_number) in header_segment.blocks.iter().zip(segment_start..) {
        if header.block_number != expected_block_number {
            warn!(
                header_number = header.block_number,
                expected_block_number, "header number mismatch"
            );
        }

        if verbose {
            let hash = models::FieldElement::from(&header.block_hash);
            let parent_hash = models::FieldElement::from(&header.parent_block_hash);
            info!(
                "header: number={}\thash=0x{:x}\tparent_hash=0x{:x}\ttimestamp={}",
                header.block_number, hash, parent_hash, header.timestamp
            );
        }
    }

    Ok(())
}

async fn inspect_event(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_start = snapshot.segment_options.segment_start(block_number);
    let segment_name = snapshot.segment_options.format_segment_name(block_number);

    info!(start = segment_start, "deserializing event segment");

    let bytes = storage
        .mmap(format!("segment/{segment_name}"), "events")
        .await?;

    let segment = rkyv::from_bytes::<store::EventSegment>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse events segment")?;

    if parse_only {
        return Ok(());
    }

    if segment.blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = segment.blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "segment size mismatch"
        );
    }

    for (block, expected_block_number) in segment.blocks.iter().zip(segment_start..) {
        if block.block_number != expected_block_number {
            warn!(
                block_number = block.block_number,
                expected_block_number, "block number mismatch"
            );
        }

        info!(
            block_number = block.block_number,
            size = block.data.len(),
            "inspect events"
        );

        let mut prev_transaction_index = 0;
        for (expected_index, event) in block.data.iter().enumerate() {
            let transaction_index = event.transaction_index;

            if transaction_index < prev_transaction_index {
                warn!(
                    transaction_index,
                    prev_transaction_index, "event transaction index is out of order"
                );
            }

            if expected_index != event.event_index as usize {
                warn!(
                    log_index = event.event_index,
                    expected_index, "event index mismatch"
                );
            }

            if verbose {
                info!(
                    "event: tx_index={} event_index={}",
                    transaction_index, event.event_index,
                );

                let from_address = models::FieldElement::from(&event.from_address);
                info!("    from_address: 0x{:x}", from_address);

                let tx_hash = models::FieldElement::from(&event.transaction_hash);
                info!("    tx_hash: 0x{:x}", tx_hash);
            }

            prev_transaction_index = transaction_index;
        }
    }

    Ok(())
}
async fn inspect_message(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_start = snapshot.segment_options.segment_start(block_number);
    let segment_name = snapshot.segment_options.format_segment_name(block_number);

    info!(start = segment_start, "deserializing message segment");

    let bytes = storage
        .mmap(format!("segment/{segment_name}"), "messages")
        .await?;

    let segment = rkyv::from_bytes::<store::MessageSegment>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse message segment")?;

    if parse_only {
        return Ok(());
    }

    if segment.blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = segment.blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "segment size mismatch"
        );
    }

    for (block, expected_block_number) in segment.blocks.iter().zip(segment_start..) {
        if block.block_number != expected_block_number {
            warn!(
                block_number = block.block_number,
                expected_block_number, "block number mismatch"
            );
        }

        info!(
            block_number = block.block_number,
            size = block.data.len(),
            "inspect messages"
        );

        let mut prev_transaction_index = 0;
        for (expected_index, message) in block.data.iter().enumerate() {
            let transaction_index = message.transaction_index;

            if transaction_index < prev_transaction_index {
                warn!(
                    transaction_index,
                    prev_transaction_index, "message transaction index is out of order"
                );
            }

            if expected_index != message.message_index as usize {
                warn!(
                    log_index = message.message_index,
                    expected_index, "message index mismatch"
                );
            }

            if verbose {
                info!(
                    "message: tx_index={} message_index={}",
                    transaction_index, message.message_index,
                );

                let from_address = models::FieldElement::from(&message.from_address);
                info!("    from_address: 0x{:x}", from_address);

                let tx_hash = models::FieldElement::from(&message.transaction_hash);
                info!("    tx_hash: 0x{:x}", tx_hash);
            }

            prev_transaction_index = transaction_index;
        }
    }

    Ok(())
}

async fn inspect_receipt(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_start = snapshot.segment_options.segment_start(block_number);
    let segment_name = snapshot.segment_options.format_segment_name(block_number);

    info!(start = segment_start, "deserializing receipt segment");

    let bytes = storage
        .mmap(format!("segment/{segment_name}"), "receipt")
        .await?;

    let segment = rkyv::from_bytes::<store::TransactionReceiptSegment>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse receipt segment")?;

    if parse_only {
        return Ok(());
    }

    if segment.blocks.len() != snapshot.segment_options.segment_size {
        warn!(
            size = segment.blocks.len(),
            expected_size = snapshot.segment_options.segment_size,
            "segment size mismatch"
        );
    }

    for (block, expected_block_number) in segment.blocks.iter().zip(segment_start..) {
        if block.block_number != expected_block_number {
            warn!(
                block_number = block.block_number,
                expected_block_number, "block number mismatch"
            );
        }

        info!(
            block_number = block.block_number,
            size = block.data.len(),
            "inspect messages"
        );

        for (expected_index, receipt) in block.data.iter().enumerate() {
            let meta = receipt.meta();
            let transaction_index = meta.transaction_index;

            if expected_index != transaction_index as usize {
                warn!(
                    transaction_index = transaction_index,
                    expected_index, "transaction index mismatch"
                );
            }

            if verbose {
                use store::TransactionReceipt::*;
                let tx_hash = models::FieldElement::from(&meta.transaction_hash);
                let tx_type = match &receipt {
                    &Invoke(_) => "invoke",
                    &Declare(_) => "declare",
                    &L1Handler(_) => "l1_handler",
                    &Deploy(_) => "deploy",
                    &DeployAccount(_) => "deploy_account",
                };
                info!(
                    "receipt: tx_index={}\ttx_hash=0x{:x} type={}",
                    transaction_index, tx_hash, tx_type
                );
            }
        }
    }

    Ok(())
}

async fn inspect_transaction(
    mut storage: CachedStorage<AppStorageBackend>,
    snapshot: Snapshot,
    block_number: u64,
    verbose: bool,
    parse_only: bool,
) -> Result<()> {
    let segment_start = snapshot.segment_options.segment_start(block_number);
    let segment_name = snapshot.segment_options.format_segment_name(block_number);

    info!(start = segment_start, "deserializing transaction segment");

    let bytes = storage
        .mmap(format!("segment/{segment_name}"), "transaction")
        .await?;

    let segment = rkyv::from_bytes::<store::TransactionSegment>(&bytes)
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to parse transaction segment")?;

    if parse_only {
        return Ok(());
    }

    todo!();
}

async fn load_snapshot<S>(storage: &mut S) -> Result<Snapshot>
where
    S: StorageBackend,
    S::Reader: Unpin + Send,
{
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
