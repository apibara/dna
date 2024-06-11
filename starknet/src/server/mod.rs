use std::net::SocketAddr;

use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::SnapshotChange,
    server::CursorProducerService,
    storage::{AppStorageBackend, CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::dna::stream::dna_stream_file_descriptor_set;
use error_stack::ResultExt;
use futures_util::Stream;
use service::DnaService;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

mod filter;
mod service;

pub struct DnaServer<C>
where
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    /// Cached storage for segmented data.
    storage: CachedStorage<AppStorageBackend>,
    /// Local storage for temporary blocks.
    local_storage: LocalStorageBackend,
    /// Stream producing changes to the snapshot state.
    snapshot_changes: C,
}

impl<C> DnaServer<C>
where
    C: Stream<Item = SnapshotChange> + Unpin + Send + Sync + 'static,
{
    pub fn new(
        storage: CachedStorage<AppStorageBackend>,
        local_storage: LocalStorageBackend,
        snapshot_changes: C,
    ) -> Self {
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

        server_task
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("ingestion gRPC server error")
    }
}
