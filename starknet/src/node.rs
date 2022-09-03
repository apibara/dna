//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::{
    net::{AddrParseError, SocketAddr},
    sync::Arc,
    time::Duration,
};

use apibara_node::{
    chain_tracker::{ChainTracker, ChainTrackerError},
    db::libmdbx::{Environment, EnvironmentKind, Error as MdbxError},
};
use futures::future;
use starknet::providers::SequencerGatewayProvider;
use tokio::{sync::broadcast, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_ingestion::{BlockIngestor, BlockIngestorError},
    server::{Server, ServerError},
    status_reporter::StatusReporter,
};

#[derive(Debug)]
pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceNodeError {
    #[error("database error")]
    Database(#[from] MdbxError),
    #[error("error setting up signal handler")]
    SignalHandler(#[from] ctrlc::Error),
    #[error("error parsing url")]
    UrlParse(#[from] url::ParseError),
    #[error("error waiting tokio task")]
    Join(#[from] JoinError),
    #[error("error ingesting block")]
    BlockIngestion(#[from] BlockIngestorError),
    #[error("error tracking chain state")]
    ChainTracker(#[from] ChainTrackerError),
    #[error("node did not shutdown gracefully")]
    Shutdown,
    #[error("error parsing server address")]
    AddressParseError(#[from] AddrParseError),
    #[error("server error")]
    Server(#[from] ServerError),
}

pub type Result<T> = std::result::Result<T, SourceNodeError>;

impl<E: EnvironmentKind> StarkNetSourceNode<E> {
    pub fn new(db: Arc<Environment<E>>) -> Self {
        StarkNetSourceNode { db }
    }

    pub async fn start(self) -> Result<()> {
        // Setup cancellation for graceful shutdown
        let cts = CancellationToken::new();
        let ct = cts.clone();
        ctrlc::set_handler({
            let cts = cts.clone();
            move || {
                cts.cancel();
            }
        })?;

        let starknet_client = Arc::new(SequencerGatewayProvider::starknet_alpha_goerli());

        let chain = ChainTracker::new(self.db.clone())?;
        let chain = Arc::new(chain);

        let (block_tx, block_rx) = broadcast::channel(128);

        let server_addr: SocketAddr = "0.0.0.0:7171".parse()?;
        let server = Server::new(chain.clone());
        let mut server_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                server
                    .start(server_addr, block_rx, ct)
                    .await
                    .map_err(SourceNodeError::Server)
            }
        });

        let block_ingestor = BlockIngestor::new(chain, starknet_client)?;
        let mut block_ingestor_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                block_ingestor
                    .start(block_tx, ct)
                    .await
                    .map_err(SourceNodeError::BlockIngestion)
            }
        });

        info!("source node started");
        // Gracefully shutdown of all tasks.
        // Start by waiting for the first task that completes
        let res = tokio::select! {
            res = &mut block_ingestor_handle => {
                res
            },
            res = &mut server_handle => {
                res
            },
        };

        info!(res = ?res, "terminated");

        // Then signal to all other tasks to stop
        cts.cancel();

        // Then wait for them to complete, but not for _too long_
        // TODO: this panics because it polls a completed join handle.
        // figure out how to fuse the handle
        let all_handles = future::try_join_all([block_ingestor_handle, server_handle]);
        tokio::time::timeout(Duration::from_secs(30), all_handles)
            .await
            .map_err(|_| SourceNodeError::Shutdown)??;

        Ok(())
    }
}
