use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::SnapshotChange,
    storage::{LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::dna::stream::dna_stream_file_descriptor_set;
use error_stack::ResultExt;
use futures_util::Stream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::server::{cursor_producer::CursorProducerService, service::DnaService};

mod cursor_producer;
mod filter;
mod service;

pub struct DnaServer<S, C>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    /// Storage (possibly cached) for segmented data.
    storage: S,
    /// Local storage for temporary blocks.
    local_storage: LocalStorageBackend,
    /// Stream producing changes to the snapshot state.
    snapshot_changes: C,
}

impl<S, C> DnaServer<S, C>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(storage: S, local_storage: LocalStorageBackend, snapshot_changes: C) -> Self {
        Self {
            storage,
            local_storage,
            snapshot_changes,
        }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let cursor_producer = CursorProducerService::new(self.snapshot_changes).start(ct.clone());

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dna_stream_file_descriptor_set())
            .build()
            .change_context(DnaError::Fatal)
            .attach_printable("failed to create gRPC reflection service")?;

        let dna_service =
            DnaService::new(self.storage, self.local_storage, cursor_producer).into_service();

        info!(addr = %addr, "starting dna server");
        let server_task = TonicServer::builder()
            .add_service(reflection_service)
            .add_service(dna_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            });

        tokio::select! {
            Err(err) = server_task => {
                Err(err).change_context(DnaError::Fatal).attach_printable("ingestion gRPC server error")
            }
            else => {
                Ok(())
            }
        }
    }
}
