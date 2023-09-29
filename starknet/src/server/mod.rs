mod health;
pub mod stream;

use std::{net::SocketAddr, sync::Arc};

use apibara_core::node as node_pb;
use apibara_node::{
    db::libmdbx::{Environment, EnvironmentKind},
    server::{QuotaClientFactory, QuotaConfiguration, RequestObserver, SimpleRequestObserver},
};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::{debug_span, error, info};

use crate::{
    db::DatabaseStorage, ingestion::IngestionStreamClient, server::stream::StreamService,
    status::StatusClient,
};

use self::health::HealthReporter;

pub struct Server<E: EnvironmentKind, O: RequestObserver> {
    db: Arc<Environment<E>>,
    ingestion: Arc<IngestionStreamClient>,
    status: StatusClient,
    blocks_per_second_quota: u32,
    request_observer: O,
    quota_configuration: QuotaConfiguration,
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

impl<E, O> Server<E, O>
where
    E: EnvironmentKind,
    O: RequestObserver,
{
    pub fn new(
        db: Arc<Environment<E>>,
        ingestion: IngestionStreamClient,
        status: StatusClient,
        blocks_per_second_quota: u32,
    ) -> Server<E, SimpleRequestObserver> {
        let ingestion = Arc::new(ingestion);
        let request_observer = SimpleRequestObserver::default();
        let quota_configuration = QuotaConfiguration::NoQuota;
        Server {
            db,
            ingestion,
            status,
            request_observer,
            blocks_per_second_quota,
            quota_configuration,
        }
    }

    /// Creates a new Server with the given request observer.
    pub fn with_request_observer<S: RequestObserver>(self, request_observer: S) -> Server<E, S> {
        Server {
            db: self.db,
            ingestion: self.ingestion,
            status: self.status,
            request_observer,
            blocks_per_second_quota: self.blocks_per_second_quota,
            quota_configuration: self.quota_configuration,
        }
    }

    pub fn with_quota_configuration(mut self, config: QuotaConfiguration) -> Self {
        self.quota_configuration = config;
        self
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<(), ServerError> {
        let (mut health_reporter, health_service) = HealthReporter::new(self.db.clone());

        let reporter_handle = tokio::spawn({
            let ct = ct.clone();
            async move { health_reporter.start(ct).await }
        });

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(node_pb::v1alpha2::node_file_descriptor_set())
            .build()?;

        let quota_client_factory = QuotaClientFactory::new(self.quota_configuration);
        let storage = DatabaseStorage::new(self.db);

        let stream_service = StreamService::new(
            self.ingestion,
            self.status,
            storage,
            self.request_observer,
            self.blocks_per_second_quota,
            quota_client_factory,
        )
        .into_service();

        info!(addr = %addr, "starting server");

        TonicServer::builder()
            .trace_fn(|_| debug_span!("node_server"))
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
