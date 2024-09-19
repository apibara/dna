mod cli;
mod error;
mod service;

use std::net::SocketAddr;

use apibara_dna_protocol::dna::stream::dna_stream_file_descriptor_set;
use error::ServerError;
use error_stack::{Result, ResultExt};
use service::StreamService;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::chain_view::ChainView;
use crate::data_stream::ScannerFactory;

pub use self::cli::ServerArgs;
pub use self::service::StreamServiceOptions;

#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// The server address.
    pub address: SocketAddr,
    /// Stream service options.
    pub stream_service_options: StreamServiceOptions,
}

pub async fn server_loop<SF>(
    scanner_factory: SF,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
    options: ServerOptions,
    ct: CancellationToken,
) -> Result<(), ServerError>
where
    SF: ScannerFactory + Send + Sync + 'static,
{
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(dna_stream_file_descriptor_set())
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .change_context(ServerError)
        .attach_printable("failed to create gRPC reflection service")?;

    let stream_service = StreamService::new(
        scanner_factory,
        chain_view,
        options.stream_service_options,
        ct.clone(),
    );

    info!(address = %options.address, "starting DNA server");

    TonicServer::builder()
        .add_service(health_service)
        .add_service(reflection_service)
        .add_service(stream_service.into_service())
        .serve_with_shutdown(options.address, {
            let ct = ct.clone();
            async move { ct.cancelled().await }
        })
        .await
        .change_context(ServerError)
}
