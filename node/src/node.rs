use std::{net::SocketAddr, sync::Arc};

use libmdbx::{Environment, EnvironmentKind};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    application::Application,
    o11y::{init_opentelemetry, OpenTelemetryInitError},
    processor::{Processor, ProcessorError},
    server::{Server, ServerError},
};

pub struct Node<A: Application, E: EnvironmentKind> {
    processor: Arc<Processor<A, E>>,
    server: Server<Processor<A, E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("error processing message")]
    Processor(#[from] ProcessorError),
    #[error("grpc server error")]
    Server(#[from] ServerError),
    #[error("error configuring opentelemetry")]
    OpenTelemetry(#[from] OpenTelemetryInitError),
}

pub type Result<T> = std::result::Result<T, NodeError>;

impl<A, E> Node<A, E>
where
    A: Application,
    E: EnvironmentKind,
{
    /// Start building a new node.
    pub fn new(db: Arc<Environment<E>>, application: A) -> Result<Node<A, E>> {
        let processor = Processor::new(db.clone(), application)?;
        let processor = Arc::new(processor);
        let server = Server::new(processor.clone());

        Ok(Node { processor, server })
    }

    /// Start the node.
    pub async fn start(self) -> Result<()> {
        init_opentelemetry()?;

        // TODO: addr comes from config.
        let addr: SocketAddr = "0.0.0.0:7172".parse().unwrap();

        // Root cancellation token.
        let cts = CancellationToken::new();

        let mut processor_handle = tokio::spawn({
            let processor = self.processor.clone();
            let ct = cts.clone();
            async move { processor.start(ct).await.map_err(NodeError::Processor) }
        });

        let mut server_handle = tokio::spawn({
            let ct = cts.clone();
            async move { self.server.start(addr, ct).await.map_err(NodeError::Server) }
        });

        info!("stream node started");

        let res = tokio::select! {
            res = &mut processor_handle => {
                res
            }
            res = &mut server_handle => {
                res
            }
        };

        info!(res = ?res, "terminated");

        Ok(())
    }
}
