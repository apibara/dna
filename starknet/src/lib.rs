pub mod core;
pub mod db;
pub mod healer;
pub mod ingestion;
pub mod node;
pub mod provider;
pub mod server;
pub mod status;
pub mod stream;
pub mod websocket;

pub use crate::node::StarkNetNode;
pub use crate::provider::HttpProvider;

pub use apibara_node::{
    db::libmdbx::NoWriteMap,
    server::{MetadataKeyRequestObserver, SimpleRequestObserver},
};
use apibara_sdk::Uri;
use ingestion::BlockIngestionConfig;

use std::{fmt, path::PathBuf, time::Duration};

use apibara_node::{db::default_data_dir, server::QuotaConfiguration};
use clap::Args;
use error_stack::{Result, ResultExt};
use tempdir::TempDir;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Clone, Debug, Args)]
pub struct StartArgs {
    /// StarkNet RPC address.
    #[arg(long, env)]
    pub rpc: String,
    /// Data directory. Defaults to `$XDG_DATA_HOME`.
    #[arg(long, env)]
    pub data: Option<PathBuf>,
    /// Indexer name. Defaults to `starknet`.
    #[arg(long, env)]
    pub name: Option<String>,
    /// Head refresh interval (in milliseconds).
    #[arg(long, env)]
    pub head_refresh_interval_ms: Option<u64>,
    /// Wait for RPC to be available before starting.
    #[arg(long, env)]
    pub wait_for_rpc: bool,
    /// Set an upper bound on the number of blocks per second clients can stream.
    #[arg(long, env)]
    pub blocks_per_second_limit: Option<u32>,
    /// Create a temporary directory for data, deleted when devnet is closed.
    #[arg(long, env)]
    pub devnet: bool,
    /// Use the specified metadata key for tracing and metering.
    #[arg(long, env)]
    pub use_metadata: Vec<String>,
    #[command(flatten)]
    pub quota_server: Option<QuotaServerArgs>,
    /// Bind the DNA server to this address, defaults to `0.0.0.0:7171`.
    #[arg(long, env)]
    pub address: Option<String>,
    // Websocket address
    #[arg(long, env)]
    pub websocket_address: Option<String>,
    /// Override the ingestion starting block.
    ///
    /// This should be used only for testing and never in production.
    #[arg(long, env)]
    pub dangerously_override_ingestion_start_block: Option<u64>,
}

#[derive(Default, Clone, Debug, Args)]
pub struct QuotaServerArgs {
    /// Quota server address.
    #[arg(long, env)]
    pub quota_server_address: Option<String>,
    /// Metadata key used to identify the team.
    #[arg(long, env)]
    pub team_metadata_key: Option<String>,
    /// Metadata key used to identify the client.
    #[arg(long, env)]
    pub client_metadata_key: Option<String>,
}

/// Connect the cancellation token to the ctrl-c handler.
pub fn set_ctrlc_handler(ct: CancellationToken) -> Result<(), StarknetError> {
    ctrlc::set_handler({
        move || {
            ct.cancel();
        }
    })
    .change_context(StarknetError)
    .attach_printable("failed to setup ctrl-c handler")?;

    Ok(())
}

#[derive(Debug)]
pub struct StarknetError;
impl error_stack::Context for StarknetError {}

impl fmt::Display for StarknetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("starknet operation failed")
    }
}

pub async fn start_node(args: StartArgs, cts: CancellationToken) -> Result<(), StarknetError> {
    let mut node =
        StarkNetNode::<HttpProvider, SimpleRequestObserver, NoWriteMap>::builder(&args.rpc)
            .change_context(StarknetError)
            .attach_printable("failed to create server")?
            .with_request_observer(MetadataKeyRequestObserver::new(args.use_metadata));

    if args.devnet {
        let tempdir = TempDir::new("apibara").change_context(StarknetError)?;
        info!("starting in devnet mode");
        node.with_datadir(tempdir.path().to_path_buf());
    } else if let Some(datadir) = args.data {
        info!("using user-provided datadir");
        node.with_datadir(datadir);
    } else if let Some(name) = &args.name {
        let datadir = default_data_dir()
            .map(|p| p.join(name))
            .expect("no datadir");
        node.with_datadir(datadir);
    }

    if let Some(address) = args.address {
        node.with_address(address);
    }

    let quota_args = args.quota_server.unwrap_or_default();
    if let Some(quota_server_address) = quota_args.quota_server_address {
        let server_address = quota_server_address
            .parse::<Uri>()
            .change_context(StarknetError)
            .attach_printable("failed to parse quota address server")?;
        let network_name = args.name.unwrap_or_else(|| "starknet".to_string());
        let quota_configuration = QuotaConfiguration::RemoteQuota {
            server_address,
            network_name,
            team_metadata_key: quota_args
                .team_metadata_key
                .unwrap_or_else(|| "x-team-name".to_string()),
            client_metadata_key: quota_args.client_metadata_key,
        };
        node.with_quota_configuration(quota_configuration);
    }

    if let Some(websocket_address) = args.websocket_address {
        node.with_websocket_address(websocket_address);
    }

    if let Some(limit) = args.blocks_per_second_limit {
        node.with_blocks_per_second_limit(limit);
    }

    let mut block_ingestion_config = BlockIngestionConfig::default();

    if let Some(head_refresh_interval_free) = args.head_refresh_interval_ms {
        // Adjust to some value that makes sense.
        let head_refresh_interval = head_refresh_interval_free.clamp(100, 10_000);
        block_ingestion_config.head_refresh_interval = Duration::from_millis(head_refresh_interval);
    }

    if let Some(starting_block) = args.dangerously_override_ingestion_start_block {
        block_ingestion_config.ingestion_starting_block = Some(starting_block);
    }

    node.with_block_ingestion_config(block_ingestion_config);

    node.build()
        .change_context(StarknetError)
        .attach_printable("failed to initialize node")?
        .start(cts.clone(), args.wait_for_rpc)
        .await
        .change_context(StarknetError)
        .attach_printable("error while running starknet node")?;

    Ok(())
}
