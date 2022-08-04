//! Apibara gRPC server.

mod indexer_service;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::{
    configuration::Network, indexer::IndexerPersistence, network_manager::NetworkManager,
    persistence::NetworkName,
};

use self::indexer_service::IndexerManagerService;

pub struct Server<IP: IndexerPersistence> {
    network_manager: Arc<NetworkManager>,
    indexer_persistence: Arc<IP>,
}

impl<IP: IndexerPersistence> Server<IP> {
    pub fn new(
        networks: HashMap<NetworkName, Network>,
        indexer_persistence: Arc<IP>,
    ) -> Server<IP> {
        let network_manager = Arc::new(NetworkManager::new(networks));
        Server {
            network_manager,
            indexer_persistence,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let indexer_manager = IndexerManagerService::new(
            self.network_manager.clone(),
            self.indexer_persistence.clone(),
        );

        info!(addr=?addr, "starting apibara server");

        TonicServer::builder()
            .add_service(indexer_manager.into_service())
            .serve(addr)
            .await?;

        Ok(())
    }
}
