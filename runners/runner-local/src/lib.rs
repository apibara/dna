pub mod configuration;
pub mod error;
pub mod manager;
pub mod server;
pub mod utils;

use apibara_runner_common::runner::v1::{indexer_runner_server, runner_file_descriptor_set};
use error_stack::{Result, ResultExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::{
    configuration::Configuration,
    error::{LocalRunnerError, LocalRunnerResultExt},
    manager::IndexerManager,
    server::RunnerService,
};

pub async fn start_server(
    config: Configuration,
    ct: CancellationToken,
) -> Result<(), LocalRunnerError> {
    let indexer_manager = IndexerManager::new();
    let runner_service = RunnerService::new(indexer_manager);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    let listener = TcpListener::bind(config.address)
        .await
        .internal("failed to bind server address")
        .attach_printable_lazy(|| format!("address: {}", config.address))?;

    let local_address = listener
        .local_addr()
        .internal("failed to get local address")?;

    info!("server listening on {}", local_address);

    let listener = TcpListenerStream::new(listener);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(runner_file_descriptor_set())
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build()
        .internal("failed to create gRPC reflection service")?;

    let server_fut = TonicServer::builder()
        .add_service(reflection_service)
        .add_service(health_service)
        .add_service(runner_service.into_service())
        .serve_with_incoming_shutdown(listener, {
            let ct = ct.clone();
            async move { ct.cancelled().await }
        });

    health_reporter
        .set_serving::<indexer_runner_server::IndexerRunnerServer<RunnerService>>()
        .await;

    server_fut
        .await
        .internal("error while running local runner service")?;

    Ok(())
}
