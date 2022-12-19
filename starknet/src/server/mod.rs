mod health;
mod span;
mod stream;

use std::{net::SocketAddr, sync::Arc};

use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::{error, info, info_span};

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};

use crate::{
    core::pb, db::DatabaseStorage, ingestion::IngestionStreamClient, server::stream::StreamService,
};

use self::health::HealthReporter;

pub use self::span::{MetadataKeyRequestSpan, RequestSpan, SimpleRequestSpan};

pub struct Server<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    ingestion: Arc<IngestionStreamClient>,
    request_span: Arc<dyn RequestSpan>,
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("grpc transport error")]
    Transport(#[from] tonic::transport::Error),
    #[error("error awaiting task")]
    Task(#[from] JoinError),
    #[error("error starting reflection server")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
}

impl<E> Server<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, ingestion: IngestionStreamClient) -> Self {
        let ingestion = Arc::new(ingestion);
        let request_span = SimpleRequestSpan::default();
        Server {
            db,
            ingestion,
            request_span: Arc::new(request_span),
        }
    }

    pub fn with_request_span<S: RequestSpan>(mut self, request_span: S) -> Self {
        let request_span = Arc::new(request_span);
        self.request_span = request_span;
        self
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<(), ServerError> {
        let (mut health_reporter, health_service) = HealthReporter::new(self.db.clone());

        let reporter_handle = tokio::spawn({
            let ct = ct.clone();
            async move { health_reporter.start(ct).await }
        });

        let reflection_service =
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(
                    pb::stream::v1alpha2::stream_file_descriptor_set(),
                )
                .build()?;

        let storage = DatabaseStorage::new(self.db);
        let stream_service =
            StreamService::new(self.ingestion, storage, self.request_span).into_service();

        info!(addr = %addr, "starting server");

        TonicServer::builder()
            .trace_fn(|_| info_span!("node_server"))
            .add_service(health_service)
            .add_service(stream_service)
            .add_service(reflection_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move { ct.cancelled().await }
            })
            .await?;

        // signal health reporter to stop and wait for it
        ct.cancel();
        reporter_handle.await?;

        Ok(())
    }
}
