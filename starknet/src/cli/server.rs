use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    server::IngestionStateSyncServer,
    storage::{AppStorageBackend, CacheArgs, CachedStorage, LocalStorageBackend, StorageArgs},
};
use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::server::DnaServer;

/// Start serving ingested data to clients.
#[derive(Args, Debug)]
pub struct StartServerArgs {
    /// Location for ingested data.
    #[clap(flatten)]
    pub storage: StorageArgs,
    /// Location for cached/temporary data.
    #[clap(flatten)]
    pub cache: CacheArgs,
    #[clap(flatten)]
    pub server: ServerArgs,
}

#[derive(Args, Debug, Clone)]
pub struct ServerArgs {
    /// Ingestion server URL.
    ///
    /// Defaults to `http://127.0.0.1:7001`.
    #[clap(long, env, default_value = "http://127.0.0.1:7001")]
    pub ingestion_server: String,
    /// DNA server address.
    ///
    /// Defaults to `0.0.0.0:7007`.
    #[arg(long, env, default_value = "0.0.0.0:7007")]
    pub server_address: String,
}

pub async fn run_server(args: StartServerArgs) -> Result<()> {
    info!("Starting Starknet data server");
    let storage = args
        .storage
        .to_app_storage_backend()
        .attach_printable("failed to initialize storage backend")?;

    // TODO: check why local cache is not working.
    let local_cache_storage = args.cache.to_local_storage_backend();
    let storage = CachedStorage::new(local_cache_storage.clone(), storage, &[]);

    run_server_with_storage(args, storage, local_cache_storage).await
}

pub async fn run_server_with_storage(
    args: StartServerArgs,
    storage: CachedStorage<AppStorageBackend>,
    local_cache_storage: LocalStorageBackend,
) -> Result<()> {
    let ct = CancellationToken::new();

    let ingestion_stream = IngestionStateSyncServer::new(
        args.server.ingestion_server.clone(),
        local_cache_storage.clone(),
    )?
    .start(ct.clone());

    let server_address = args
        .server
        .server_address
        .parse::<SocketAddr>()
        .change_context(DnaError::Configuration)
        .attach_printable("failed to parse server address")?;

    DnaServer::new(storage, local_cache_storage, ingestion_stream)
        .start(server_address, ct)
        .await
        .change_context(DnaError::Fatal)
        .attach_printable("error inside DNA server")
}
