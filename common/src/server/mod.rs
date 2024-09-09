mod cli;
mod error;
mod service;

use std::{net::SocketAddr, path::PathBuf};

use apibara_dna_protocol::dna::stream::dna_stream_file_descriptor_set;
use apibara_etcd::EtcdClient;
use error::ServerError;
use error_stack::{Result, ResultExt};
use futures::TryFutureExt;
use service::StreamService;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::{chain_view::chain_view_sync_loop, object_store::ObjectStore};

pub use self::cli::ServerArgs;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// The server address.
    pub address: SocketAddr,
    /// Directory to store cached data.
    pub cache_dir: PathBuf,
}

pub async fn server_loop(
    etcd_client: EtcdClient,
    object_store: ObjectStore,
    options: ServerOptions,
    ct: CancellationToken,
) -> Result<(), ServerError> {
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dna_stream_file_descriptor_set())
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .change_context(ServerError)
        .attach_printable("failed to create gRPC reflection service")?;

    let (chain_view, chain_view_sync) = chain_view_sync_loop(etcd_client, object_store.clone())
        .await
        .change_context(ServerError)
        .attach_printable("failed to start chain view sync service")?;

    let sync_handle = tokio::spawn(chain_view_sync.start(ct.clone()).or_else(|err| async {
        Err(err)
            .change_context(ServerError)
            .attach_printable("chain view sync error")
    }));

    let stream_service = StreamService::new(chain_view);

    info!(address = %options.address, "starting DNA server");

    let server_task = tokio::spawn(
        TonicServer::builder()
            .add_service(health_service)
            .add_service(reflection_service)
            .add_service(stream_service.into_service())
            .serve_with_shutdown(options.address, {
                let ct = ct.clone();
                async move { ct.cancelled().await }
            })
            .or_else(|err| async { Err(err).change_context(ServerError) }),
    );

    match tokio::try_join!(server_task, sync_handle).change_context(ServerError)? {
        (Err(err), _) => return Err(err).change_context(ServerError),
        (_, Err(err)) => return Err(err).change_context(ServerError),
        _ => {}
    }

    Ok(())
}

impl Default for ServerOptions {
    fn default() -> Self {
        let address = "0.0.0.0:7007".parse().expect("failed to parse address");
        let cache_dir = dirs::data_local_dir()
            .expect("failed to get data dir")
            .join("dna-v2");
        Self { address, cache_dir }
    }
}
