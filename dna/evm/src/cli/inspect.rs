use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::Snapshot,
    storage::{AppStorageBackend, StorageBackend},
};
use clap::Args;
use error_stack::ResultExt;
use roaring::RoaringBitmap;
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

use crate::segment::SegmentGroupReader;

use super::common::StorageArgs;

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
