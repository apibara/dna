//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::{
    fs,
    net::{AddrParseError, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use apibara_node::{
    chain_tracker::{ChainTracker, ChainTrackerError},
    db::libmdbx::{Environment, EnvironmentKind, Error as MdbxError, Geometry, NoWriteMap},
};
use futures::future;
use starknet::providers::SequencerGatewayProvider;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_ingestion::{BlockIngestor, BlockIngestorError},
    server::{Server, ServerError},
};

pub async fn start_starknet_source_node(datadir: PathBuf, gateway: SequencerGateway) -> Result<()> {
    // setup db
    fs::create_dir_all(&datadir)?;
    let min_size = byte_unit::n_gib_bytes!(10) as usize;
    let max_size = byte_unit::n_gib_bytes!(100) as usize;
    let growth_step = byte_unit::n_gib_bytes(2) as isize;
    let db = Environment::<NoWriteMap>::new()
        .set_geometry(Geometry {
            size: Some(min_size..max_size),
            growth_step: Some(growth_step),
            shrink_threshold: None,
            page_size: None,
        })
        .set_max_dbs(100)
        .open(&datadir)?;
    let db = Arc::new(db);

    // setup gateway client
    let starknet_client = match gateway {
        SequencerGateway::GoerliTestnet => SequencerGatewayProvider::starknet_alpha_goerli(),
        SequencerGateway::Mainnet => SequencerGatewayProvider::starknet_alpha_mainnet(),
    };
    let starknet_client = Arc::new(starknet_client);

    let node = StarkNetSourceNode::new(db, starknet_client);
    node.start().await?;
    Ok(())
}

pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    sequencer_provider: Arc<SequencerGatewayProvider>,
}

/// StarkNet sequencer gateway address.
#[derive(Debug, Clone)]
pub enum SequencerGateway {
    GoerliTestnet,
    Mainnet,
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
    #[error("io error")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, SourceNodeError>;

impl<E: EnvironmentKind> StarkNetSourceNode<E> {
    pub fn new(db: Arc<Environment<E>>, sequencer_provider: Arc<SequencerGatewayProvider>) -> Self {
        StarkNetSourceNode {
            db,
            sequencer_provider,
        }
    }

    pub async fn start(self) -> Result<()> {
        info!("starting source node");
        // Setup cancellation for graceful shutdown
        let cts = CancellationToken::new();
        let ct = cts.clone();
        ctrlc::set_handler({
            let cts = cts.clone();
            move || {
                cts.cancel();
            }
        })?;

        let chain = ChainTracker::new(self.db.clone())?;
        let chain = Arc::new(chain);

        let block_ingestor = BlockIngestor::new(chain.clone(), self.sequencer_provider)?;
        let block_ingestor = Arc::new(block_ingestor);
        let mut block_ingestor_handle = tokio::spawn({
            let ct = ct.clone();
            let block_ingestor = block_ingestor.clone();
            async move {
                block_ingestor
                    .start(ct)
                    .await
                    .map_err(SourceNodeError::BlockIngestion)
            }
        });

        let server_addr: SocketAddr = "0.0.0.0:7171".parse()?;
        let server = Server::new(self.db.clone(), chain.clone(), block_ingestor.clone());
        let mut server_handle = tokio::spawn({
            let ct = ct.clone();
            async move {
                server
                    .start(server_addr, ct)
                    .await
                    .map_err(SourceNodeError::Server)
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
