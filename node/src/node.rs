use std::sync::Arc;

use libmdbx::{Environment, EnvironmentKind};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    application::Application,
    o11y::{init_opentelemetry, OpenTelemetryInitError},
    processor::{Processor, ProcessorError},
};

pub struct Node<A: Application, E: EnvironmentKind> {
    processor: Arc<Processor<A, E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("error processing message")]
    Processor(#[from] ProcessorError),
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
        let processor = Processor::new(db, application)?;

        Ok(Node {
            processor: Arc::new(processor),
        })
    }

    /// Start the node.
    pub async fn start(self) -> Result<()> {
        init_opentelemetry()?;

        // Root cancellation token.
        let cts = CancellationToken::new();

        let mut processor_handle = tokio::spawn({
            let processor = self.processor.clone();
            let ct = cts.clone();
            async move { processor.start(ct).await.map_err(NodeError::Processor) }
        });

        info!("stream node started");
        let res = tokio::select! {
            res = &mut processor_handle => {
                res
            }
        };

        info!(res = ?res, "terminated");

        Ok(())
    }
}
