mod cli;
mod error;

use std::{net::SocketAddr, path::PathBuf};

use apibara_dna_protocol::dna::stream::dna_stream_file_descriptor_set;
use error::ServerError;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

pub use self::cli::ServerArgs;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// The server address.
    pub address: SocketAddr,
    /// Directory to store cached data.
    pub cache_dir: PathBuf,
}

pub async fn server_loop(options: ServerOptions, ct: CancellationToken) -> Result<(), ServerError> {
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dna_stream_file_descriptor_set())
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .change_context(ServerError)
        .attach_printable("failed to create gRPC reflection service")?;

    info!(address = %options.address, "starting DNA server");

    TonicServer::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .serve_with_shutdown(options.address, {
            let ct = ct.clone();
            async move { ct.cancelled().await }
        })
        .await
        .change_context(ServerError)
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
