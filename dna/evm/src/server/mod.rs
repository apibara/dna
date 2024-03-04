mod service;

use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    storage::StorageBackend,
};
use apibara_dna_protocol::ingestion::{ingestion_client::IngestionClient, SubscribeRequest};
use error_stack::ResultExt;
use futures_util::{StreamExt, TryStreamExt};
use tokio::pin;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::server::service::Service;

pub struct DnaServer<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    ingestion_server: String,
    storage: S,
}

impl<S> DnaServer<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(ingestion_server: impl Into<String>, storage: S) -> Self {
        Self {
            ingestion_server: ingestion_server.into(),
            storage,
        }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let mut client = IngestionClient::connect(self.ingestion_server)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to connect to ingestion server")?;

        let ingestion_changes = client
            .subscribe(SubscribeRequest::default())
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to subscribe to ingestion changes")?
            .into_inner()
            .take_until({
                let ct = ct.clone();
                async move { ct.cancelled().await }
            });

        pin!(ingestion_changes);

        let Some(message) = ingestion_changes
            .try_next()
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to read ingestion stream")?
            .and_then(|m| m.as_snapshot().cloned())
        else {
            return Err(DnaError::Fatal)
                .attach_printable("no snapshot received")
                .change_context(DnaError::Fatal);
        };

        info!(message = ?message, "received snapshot");

        let dna_service = Service::new(self.storage).into_service();

        let server_task = TonicServer::builder()
            .add_service(dna_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            });

        server_task
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("server error")?;

        Ok(())
    }
}
