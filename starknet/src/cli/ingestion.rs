use std::net::SocketAddr;

use apibara_dna_common::{
    cli::IngestionArgs,
    ingestion::{
        BlockIngestionDriver, BlockIngestor, IngestionServer, IngestionState, Segmenter, Snapshot,
        SnapshotManager,
    },
    storage::{CacheArgs, StorageArgs, StorageBackend},
};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    error::DnaStarknetError,
    ingestion::{
        StarknetCursorProvider, StarknetCursorProviderOptions, StarknetSegmentBuilder,
        StarknetSingleBlockIngestion,
    },
};

use super::common::RpcArgs;

/// Start ingesting data from Starknet.
///
/// If a snapshot is already present, it will be used to resume ingestion.
#[derive(Args, Debug)]
pub struct StartIngestionArgs {
    /// Ingestion server address.
    ///
    /// Defaults to `0.0.0.0:7001`.
    #[arg(long, env, default_value = "0.0.0.0:7001")]
    pub server_address: String,
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

pub async fn run_ingestion(args: StartIngestionArgs) -> Result<(), DnaStarknetError> {
    info!("Starting Starknet ingestion");
    let storage = args
        .storage
        .to_app_storage_backend()
        .change_context(DnaStarknetError::Configuration)
        .attach_printable("failed to initialize storage backend")?;

    run_ingestion_with_storage(args, storage).await
}

async fn run_ingestion_with_storage<S>(
    args: StartIngestionArgs,
    storage: S,
) -> Result<(), DnaStarknetError>
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
{
    let ct = CancellationToken::new();
    let segment_options = args.ingestion.segment.to_segment_options();

    let address = args
        .server_address
        .parse::<SocketAddr>()
        .change_context(DnaStarknetError::Configuration)
        .attach_printable_lazy(|| format!("failed to parse address: {}", args.server_address))?;

    let local_cache_storage = args.cache.to_local_storage_backend();
    let provider_factory = args.rpc.to_json_rpc_provider_factory()?;
    let snapshot_manager = SnapshotManager::new(storage.clone());

    let has_existing_snapshot = snapshot_manager
        .read()
        .await
        .change_context(DnaStarknetError::Fatal)?
        .is_some();

    // If a starting block is provided, create an initial snapshot.
    if let Some(starting_block) = args.ingestion.starting_block {
        if starting_block > 0 && !has_existing_snapshot {
            let starting_block = segment_options.segment_group_start(starting_block);
            let initial_snapshot = Snapshot {
                revision: 0,
                segment_options: segment_options.clone(),
                ingestion: IngestionState {
                    first_block_number: starting_block,
                    extra_segment_count: 0,
                    group_count: 0,
                },
            };
            snapshot_manager
                .write(&initial_snapshot)
                .await
                .change_context(DnaStarknetError::Fatal)?;
        }
    }

    let cursor_provider = {
        let provider = provider_factory.new_provider();
        let options = StarknetCursorProviderOptions::default();
        StarknetCursorProvider::new(provider, options)
    };

    let (cursor_stream, ingestion_driver_handle) = BlockIngestionDriver::new(
        cursor_provider,
        snapshot_manager.clone(),
        Default::default(),
    )
    .start(ct.clone());

    let starknet_ingestion = {
        let provider = provider_factory.new_provider();
        StarknetSingleBlockIngestion::new(provider)
    };

    let (ingestion_stream, ingestion_handle) = BlockIngestor::new(
        local_cache_storage.clone(),
        starknet_ingestion,
        cursor_stream,
        Default::default(),
    )
    .start(ct.clone());

    let segment_builder = StarknetSegmentBuilder::new();

    let (snapshot_changes, segmenter_handle) = Segmenter::new(
        segment_builder,
        segment_options,
        local_cache_storage.clone(),
        storage.clone(),
        snapshot_manager,
        ingestion_stream,
        Default::default(),
    )
    .start(ct.clone());

    let (server_handle, sync_handle) = IngestionServer::new(local_cache_storage, snapshot_changes)
        .start(address, ct)
        .change_context(DnaStarknetError::Fatal)?;

    tokio::select! {
        Ok(Err(err)) = ingestion_driver_handle => {
            Err(err)
                .change_context(DnaStarknetError::Fatal) .attach_printable("ingestion driver ended unexpectedly")
        }
        Ok(Err(err)) = ingestion_handle => {
            Err(err)
                .change_context(DnaStarknetError::Fatal)
                .attach_printable("ingestion stream ended unexpectedly")
        }
        Ok(Err(err)) = segmenter_handle => {
            Err(err)
                .change_context(DnaStarknetError::Fatal)
                .attach_printable("segmenter ended unexpectedly")
        }
        Ok(Err(err)) = server_handle => {
            Err(err)
                .change_context(DnaStarknetError::Fatal)
                .attach_printable("ingestion server (tonic server) ended unexpectedly")
        }
        Ok(Err(err)) = sync_handle => {
            Err(err)
                .change_context(DnaStarknetError::Fatal)
                .attach_printable("ingestion server (update loop) ended unexpectedly")
        }
        else => {
            Ok(())
        }
    }
}
