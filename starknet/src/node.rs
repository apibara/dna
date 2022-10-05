//! # StarkNet source node
//!
//! This node indexes all StarkNet blocks and produces a stream of
//! blocks with transaction data.
use std::{
    collections::HashMap,
    fs,
    net::{AddrParseError, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use apibara_node::{
    chain_tracker::{ChainTracker, ChainTrackerError},
    db::{
        libmdbx::{Environment, EnvironmentKind, Error as MdbxError, NoWriteMap},
        MdbxEnvironmentExt,
    },
};
use futures::future;
use starknet::providers::SequencerGatewayProvider;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

use crate::{
    block_ingestion::{BlockIngestor, BlockIngestorError},
    server::{Server, ServerError},
};

pub async fn start_starknet_source_node(
    datadir: PathBuf,
    gateway: SequencerGateway,
    poll_interval: Duration,
) -> Result<()> {
    // setup db
    let datadir = datadir.join(gateway.network_name());
    fs::create_dir_all(&datadir)?;
    let db = Environment::<NoWriteMap>::builder()
        .with_size_gib(10, 100)
        .with_growth_step_gib(2)
        .open(&datadir)?;
    let db = Arc::new(db);

    // setup gateway client
    let (gateway_url, feeder_gateway_url) = gateway.gateway_url_pair()?;
    let starknet_client = SequencerGatewayProvider::new(gateway_url, feeder_gateway_url);
    let starknet_client = Arc::new(starknet_client);

    let node = StarkNetSourceNode::new(db, starknet_client, poll_interval);
    node.start().await?;
    Ok(())
}

pub struct StarkNetSourceNode<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    sequencer_provider: Arc<SequencerGatewayProvider>,
    poll_interval: Duration,
}

/// StarkNet sequencer gateway address.
#[derive(Debug, Clone)]
pub enum SequencerGateway {
    GoerliTestnet,
    Mainnet,
    Custom { url: String, name: Option<String> },
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
    BlockIngestion(#[from] Box<BlockIngestorError>),
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
    #[error("error parsing custom gateway configuration")]
    CustomGatewayConfiguration,
}

pub type Result<T> = std::result::Result<T, SourceNodeError>;

impl<E: EnvironmentKind> StarkNetSourceNode<E> {
    pub fn new(
        db: Arc<Environment<E>>,
        sequencer_provider: Arc<SequencerGatewayProvider>,
        poll_interval: Duration,
    ) -> Self {
        StarkNetSourceNode {
            db,
            sequencer_provider,
            poll_interval,
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

        let block_ingestor = BlockIngestor::new(chain.clone(), self.sequencer_provider)
            .map_err(|err| SourceNodeError::BlockIngestion(Box::new(err)))?;
        let block_ingestor = Arc::new(block_ingestor);
        let mut block_ingestor_handle = tokio::spawn({
            let ct = ct.clone();
            let block_ingestor = block_ingestor.clone();
            async move {
                block_ingestor
                    .start(ct, self.poll_interval)
                    .await
                    .map_err(|err| SourceNodeError::BlockIngestion(Box::new(err)))
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

impl SequencerGateway {
    /// Creates a `SequencerGateway` from a string.
    ///
    /// `custom` can be an http or https url, in that case the sequencer will
    /// be called "custom".
    /// If you pass a string with the format `url=http://...,name=my-name`, the
    /// sequencer will use the provided url and name.
    pub fn from_custom_str(custom: &str) -> Result<SequencerGateway> {
        // user passed a url.
        if custom.starts_with("http://") || custom.starts_with("https://") {
            return Ok(SequencerGateway::Custom {
                url: custom.to_string(),
                name: None,
            });
        }
        let mut config_map: HashMap<_, _> = custom
            .split_terminator(',')
            .filter_map(|entry| {
                let mut parts = entry.splitn(2, '=');
                let key = parts.next()?.trim();
                let value = parts.next()?.trim();
                Some((key.to_owned(), value.to_owned()))
            })
            .collect();

        let url = config_map
            .remove("url")
            .ok_or(SourceNodeError::CustomGatewayConfiguration)?;
        let name = config_map.remove("name");
        Ok(SequencerGateway::Custom { url, name })
    }

    /// Returns the gateway and feeder gateway urls.
    pub fn gateway_url_pair(&self) -> Result<(Url, Url)> {
        match self {
            SequencerGateway::Mainnet => Ok((
                Url::parse("https://alpha-mainnet.starknet.io/gateway").unwrap(),
                Url::parse("https://alpha-mainnet.starknet.io/feeder_gateway").unwrap(),
            )),
            SequencerGateway::GoerliTestnet => Ok((
                Url::parse("https://alpha4.starknet.io/gateway").unwrap(),
                Url::parse("https://alpha4.starknet.io/feeder_gateway").unwrap(),
            )),
            SequencerGateway::Custom { url, .. } => {
                let base_url = Url::parse(url)?;
                let gateway_url = base_url.join("gateway")?;
                let feeder_gateway_url = base_url.join("feeder_gateway")?;
                Ok((gateway_url, feeder_gateway_url))
            }
        }
    }

    pub fn network_name(&self) -> &str {
        match self {
            SequencerGateway::Mainnet => "mainnet",
            SequencerGateway::GoerliTestnet => "testnet",
            SequencerGateway::Custom { name, .. } => name.as_deref().unwrap_or("custom"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SequencerGateway;

    #[test]
    pub fn test_sequencer_gateway_from_url() {
        let gateway = SequencerGateway::from_custom_str("https://example.org/sn/").unwrap();
        let (gateway_url, feeder_gateway_url) = gateway.gateway_url_pair().unwrap();
        assert_eq!(gateway_url.as_str(), "https://example.org/sn/gateway");
        assert_eq!(
            feeder_gateway_url.as_str(),
            "https://example.org/sn/feeder_gateway"
        );
        assert_eq!(gateway.network_name(), "custom");
    }

    #[test]
    pub fn test_sequencer_gateway_from_url_without_name() {
        let gateway = SequencerGateway::from_custom_str("url=https://example.org/sn/").unwrap();
        let (gateway_url, feeder_gateway_url) = gateway.gateway_url_pair().unwrap();
        assert_eq!(gateway_url.as_str(), "https://example.org/sn/gateway");
        assert_eq!(
            feeder_gateway_url.as_str(),
            "https://example.org/sn/feeder_gateway"
        );
        assert_eq!(gateway.network_name(), "custom");
    }

    #[test]
    pub fn test_sequencer_gateway_from_url_with_name() {
        let gateway =
            SequencerGateway::from_custom_str("name=abcd,url=https://example.org/sn/").unwrap();
        let (gateway_url, feeder_gateway_url) = gateway.gateway_url_pair().unwrap();
        assert_eq!(gateway_url.as_str(), "https://example.org/sn/gateway");
        assert_eq!(
            feeder_gateway_url.as_str(),
            "https://example.org/sn/feeder_gateway"
        );
        assert_eq!(gateway.network_name(), "abcd");
    }
}
