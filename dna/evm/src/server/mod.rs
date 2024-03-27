mod service;

use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    server::SnapshotSyncService,
    storage::StorageBackend,
};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;

use crate::server::service::Service;

pub struct DnaServer<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    ingestion_server_address: String,
    storage: S,
}

impl<S> DnaServer<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(ingestion_server_address: impl Into<String>, storage: S) -> Self {
        Self {
            ingestion_server_address: ingestion_server_address.into(),
            storage,
        }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let snapshot_sync_service = SnapshotSyncService::new(self.ingestion_server_address)?;

        let snapshot_sync_client = snapshot_sync_service.start(ct.clone());

        let dna_service = Service::new(self.storage, snapshot_sync_client).into_service();

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
