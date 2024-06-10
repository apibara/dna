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
        // return inspect_header(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.event {
        // return inspect_log(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.receipt {
        // return inspect_receipt(storage, snapshot, block_number, args.verbose).await;
    } else if let Some(block_number) = args.entity.transaction {
        // return inspect_transaction(storage, snapshot, block_number, args.verbose).await;
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
