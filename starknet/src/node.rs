//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind, Error as MdbxError};
use starknet::providers::SequencerGatewayProvider;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

use crate::{
    block_ingestion::{BlockIngestor, BlockIngestorError},
    chain_tracker::StarkNetChainTrackerError,
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
        ctrlc::set_handler(move || {
            cts.cancel();
        })?;

        let starknet_client = Arc::new(SequencerGatewayProvider::starknet_alpha_goerli());
        let block_ingestor = BlockIngestor::new(self.db.clone(), starknet_client);

        let block_ingestor_handle =
            tokio::spawn(async move { block_ingestor.start(ct.clone()).await });

        block_ingestor_handle.await??;
        Ok(())
    }
}
