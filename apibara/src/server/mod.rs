//! Apibara gRPC server.

mod indexer_service;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::{chain::ChainProvider, indexer::IndexerPersistence};

use self::indexer_service::IndexerManagerService;

pub struct Server<P: ChainProvider, IP: IndexerPersistence> {
    provider: Arc<P>,
    indexer_persistence: Arc<IP>,
}

impl<P: ChainProvider, IP: IndexerPersistence> Server<P, IP> {
    pub fn new(provider: Arc<P>, indexer_persistence: Arc<IP>) -> Server<P, IP> {
        Server {
            provider,
            indexer_persistence,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        // TODO: provider manager
        let indexer_manager =
            IndexerManagerService::new(self.provider.clone(), self.indexer_persistence.clone());

        info!(addr=?addr, "starting apibara server");

        let _ = TonicServer::builder()
            .add_service(indexer_manager.into_service())
            .serve(addr)
            .await?;

        Ok(())
    }
}
