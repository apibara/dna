//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::{sync::Arc, time::Duration};

use apibara_node::db::libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use futures::future;
use starknet::providers::SequencerGatewayProvider;
use tokio::{sync::broadcast, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_ingestion::{BlockIngestor, BlockIngestorError},
    chain_tracker::StarkNetChainTrackerError,
    storage::{BlockStorage, BlockStorageError},
};

#[derive(Debug)]
pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceNodeError {
    #[error("error tracking the chain state")]
    ChainTracker(#[from] StarkNetChainTrackerError),
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
    #[error("error storing blocks")]
    BlockStorage(#[from] BlockStorageError),
    #[error("node did not shutdown gracefully")]
    Shutdown,
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
        let (block_tx, block_rx) = broadcast::channel(128);

        let block_ingestor = BlockIngestor::new(self.db.clone(), starknet_client)?;
        let mut block_ingestor_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                block_ingestor
                    .start(block_tx, ct)
                    .await
                    .map_err(SourceNodeError::BlockIngestion)
            }
        });

        let storage = BlockStorage::new(self.db.clone())?;

        let mut storage_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                storage
                    .start(block_rx, ct)
                    .await
                    .map_err(SourceNodeError::BlockStorage)
            }
        });

        info!("source node started");
        // Gracefully shutdown of all tasks.
        // Start by waiting for the first task that completes
        tokio::select! {
            _ = &mut block_ingestor_handle => {},
            _ = &mut storage_handle => {},
        }

        // Then signal to all other tasks to stop
        cts.cancel();

        // Then wait for them to complete, but not for _too long_
        // TODO: this panics because it polls a completed join handle.
        // figure out how to fuse the handle
        let all_handles = future::try_join_all([block_ingestor_handle, storage_handle]);
        tokio::time::timeout(Duration::from_secs(30), all_handles)
            .await
            .map_err(|_| SourceNodeError::Shutdown)??;

        Ok(())
    }
}
