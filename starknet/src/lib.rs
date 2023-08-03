pub mod core;
pub mod db;
pub mod healer;
pub mod ingestion;
pub mod node;
pub mod provider;
pub mod server;
pub mod stream;
pub mod websocket;

pub use crate::node::StarkNetNode;
pub use crate::provider::HttpProvider;

pub use apibara_node::{
    db::libmdbx::NoWriteMap,
    server::{MetadataKeyRequestObserver, SimpleRequestObserver},
};
use ingestion::BlockIngestionConfig;

use std::{path::PathBuf, time::Duration};

use apibara_node::db::default_data_dir;
use clap::Args;
use color_eyre::eyre::Result;
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
    // Websocket address
    #[arg(long, env)]
    pub websocket_address: Option<String>,
}

/// Connect the cancellation token to the ctrl-c handler.
pub fn set_ctrlc_handler(ct: CancellationToken) -> Result<()> {
    ctrlc::set_handler({
        move || {
            ct.cancel();
        }
    })?;

    Ok(())
}

pub async fn start_node(args: StartArgs, cts: CancellationToken) -> Result<()> {
    let mut node =
        StarkNetNode::<HttpProvider, SimpleRequestObserver, NoWriteMap>::builder(&args.rpc)?
            .with_request_observer(MetadataKeyRequestObserver::new(args.use_metadata));

    if args.devnet {
        let tempdir = TempDir::new("apibara")?;
        info!("starting in devnet mode");
        node.with_datadir(tempdir.path().to_path_buf());
    } else if let Some(datadir) = args.data {
        info!("using user-provided datadir");
        node.with_datadir(datadir);
    } else if let Some(name) = args.name {
        let datadir = default_data_dir()
            .map(|p| p.join(name))
            .expect("no datadir");
        node.with_datadir(datadir);
    }

    if let Some(websocket_address) = args.websocket_address {
        node.with_websocket_address(websocket_address);
    }

    if let Some(limit) = args.blocks_per_second_limit {
        node.with_blocks_per_second_limit(limit);
    }

    if let Some(head_refresh_interval_free) = args.head_refresh_interval_ms {
        // Adjust to some value that makes sense.
        let head_refresh_interval = head_refresh_interval_free.clamp(100, 10_000);
        node.with_block_ingestion_config(BlockIngestionConfig {
            head_refresh_interval: Duration::from_millis(head_refresh_interval),
            ..Default::default()
        });
    }

    node.build()?.start(cts.clone(), args.wait_for_rpc).await?;

    Ok(())
}
