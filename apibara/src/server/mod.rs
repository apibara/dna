//! Apibara gRPC server.

mod indexer_service;

use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::{chain::starknet::StarkNetProvider, indexer::IndexerPersistence};

use self::indexer_service::IndexerManagerService;

pub struct Server<IP: IndexerPersistence> {
    indexer_persistence: Arc<IP>,
}

impl<IP: IndexerPersistence> Server<IP> {
    pub fn new(indexer_persistence: Arc<IP>) -> Server<IP> {
        Server {
            indexer_persistence,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        // TODO: provider manager
        let provider = StarkNetProvider::new("http://localhost:9545")?;
        let indexer_manager =
            IndexerManagerService::new(Arc::new(provider), self.indexer_persistence.clone());

        info!(addr=?addr, "starting apibara server");

        let _ = TonicServer::builder()
            .add_service(indexer_manager.into_service())
            .serve(addr)
            .await?;

        Ok(())
    }
}
