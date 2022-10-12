use std::{net::SocketAddr, sync::Arc};

use libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    application::Application,
    db::{tables, MdbxRWTransactionExt, MdbxTransactionExt},
    o11y::{init_opentelemetry, OpenTelemetryInitError},
    processor::{Processor, ProcessorError},
    server::{Server, ServerError},
};

pub struct Node<A: Application, E: EnvironmentKind> {
    configuration: NodeConfiguration<E>,
    processor: Arc<Processor<A, E>>,
    server: Server<Processor<A, E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("error reading or writing configuration")]
    Configuration(#[from] MdbxError),
    #[error("error fetching configuration from application")]
    Application(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
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
        let configuration = NodeConfiguration::new(db.clone())?;
        let processor = Processor::new(db, application)?;
        let processor = Arc::new(processor);
        let server = Server::new(processor.clone());

        Ok(Node {
            configuration,
            processor,
            server,
        })
    }

    /// Changes the gRPC server address.
    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.server = self.server.with_address(address);
        self
    }

    /// Start the node.
    pub async fn start(self) -> Result<()> {
        init_opentelemetry()?;

        let configuration = self.init_configuration().await?;

        // Root cancellation token.
        let cts = CancellationToken::new();

        let mut processor_handle = tokio::spawn({
            let processor = self.processor.clone();
            let ct = cts.clone();
            async move {
                processor
                    .start(&configuration.inputs, ct)
                    .await
                    .map_err(NodeError::Processor)
            }
        });

        let mut server_handle = match configuration.output {
            None => {
                // an application without an output is a sink.
                todo!()
            }
            Some(output) => tokio::spawn({
                let ct = cts.clone();
                async move {
                    self.server
                        .start(&output, ct)
                        .await
                        .map_err(NodeError::Server)
                }
            }),
        };

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

    async fn init_configuration(&self) -> Result<tables::Configuration> {
        if let Some(configuration) = self.configuration.read()? {
            return Ok(configuration);
        }

        let mut application = self.processor.application().await;
        let init_response = application
            .init()
            .await
            .map_err(|err| NodeError::Application(Box::new(err)))?;

        let configuration = tables::Configuration {
            output: init_response.output,
            inputs: init_response.inputs,
        };

        self.configuration.write(&configuration)?;

        Ok(configuration)
    }
}

/// Holds the [Node] configuration.
struct NodeConfiguration<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

type ConfigurationResult<T> = std::result::Result<T, MdbxError>;

impl<E: EnvironmentKind> NodeConfiguration<E> {
    pub fn new(db: Arc<Environment<E>>) -> ConfigurationResult<Self> {
        let txn = db.begin_rw_txn()?;
        txn.ensure_table::<tables::ConfigurationTable>(None)?;
        txn.commit()?;
        Ok(NodeConfiguration { db })
    }

    /// Returns the stored configuration value.
    pub fn read(&self) -> ConfigurationResult<Option<tables::Configuration>> {
        let txn = self.db.begin_ro_txn()?;
        let mut cursor = txn.open_table::<tables::ConfigurationTable>()?.cursor()?;
        let config = cursor.seek_exact(&())?.map(|v| v.1);
        txn.commit()?;
        Ok(config)
    }

    /// Writes the given configuration value.
    pub fn write(&self, config: &tables::Configuration) -> ConfigurationResult<()> {
        let txn = self.db.begin_rw_txn()?;
        let mut cursor = txn.open_table::<tables::ConfigurationTable>()?.cursor()?;
        cursor.seek_exact(&())?;
        cursor.put(&(), config)?;
        txn.commit()?;
        Ok(())
    }
}
