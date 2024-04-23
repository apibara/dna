use apibara_dna_common::{
    error::Result, server::IngestionStateSyncServer, storage::StorageBackend,
};
use clap::Args;
use error_stack::ResultExt;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use super::common::{CacheArgs, StorageArgs};

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
    #[clap(long, env)]
    pub ingestion_server: String,
}

pub async fn run_server(args: StartServerArgs) -> Result<()> {
    info!("Starting EVM data server");
    let storage = args
        .storage
        .to_app_storage_backend()
        .attach_printable("failed to initialize storage backend")?;

    run_server_with_storage(args, storage).await
}

pub async fn run_server_with_storage<S>(args: StartServerArgs, _storage: S) -> Result<()>
where
    S: StorageBackend + Clone + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    <S as StorageBackend>::Writer: Send,
{
    let ct = CancellationToken::new();

    let local_cache_storage = args.cache.to_local_storage_backend();

    let mut ingestion_stream = IngestionStateSyncServer::new(
        args.server.ingestion_server.clone(),
        local_cache_storage.clone(),
    )?
    .start(ct.clone());

    while let Some(change) = ingestion_stream.next().await {
        println!("{:?}", change);
    }

    Ok(())
}
