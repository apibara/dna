//! Apibara gRPC server.

mod application_service;

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::application::ApplicationPersistence;

use self::application_service::ApplicationManagerService;

pub struct Server {
    application_persistence: Arc<dyn ApplicationPersistence>,
}

impl Server {
    pub fn new(application_persistence: Arc<dyn ApplicationPersistence>) -> Server {
        Server {
            application_persistence,
        }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        let application_manager =
            ApplicationManagerService::new(self.application_persistence.clone());

        info!(addr=?addr, "starting apibara server");

        let _ = TonicServer::builder()
            .add_service(application_manager.into_service())
            .serve(addr)
            .await?;

        Ok(())
    }
}
