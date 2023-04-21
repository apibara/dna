use std::path::PathBuf;

use anyhow::Result;
use apibara_node::{db::default_data_dir, o11y::init_opentelemetry};
use apibara_starknet::{
    server::{MetadataKeyRequestObserver, SimpleRequestObserver},
    HttpProvider, NoWriteMap, StarkNetNode,
};
use clap::{Args, Parser, Subcommand};
use tempdir::TempDir;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Start the StarkNet source node.
    Start(StartCommand),
}

#[derive(Args)]
struct StartCommand {
    /// StarkNet RPC address.
    #[arg(long, env)]
    rpc: String,
    /// Data directory. Defaults to `$XDG_DATA_HOME`.
    #[arg(long, env)]
    data: Option<PathBuf>,
    /// Indexer name. Defaults to `starknet`.
    #[arg(long, env)]
    name: Option<String>,
    /// Wait for RPC to be available before starting.
    #[arg(long, env)]
    wait_for_rpc: bool,
    /// Create a temporary directory for data, deleted when devnet is closed.
    #[arg(long, env)]
    devnet: bool,
}

async fn start(args: StartCommand) -> Result<()> {
    init_opentelemetry()?;

    let mut node =
        StarkNetNode::<HttpProvider, SimpleRequestObserver, NoWriteMap>::builder(&args.rpc)?
            .with_request_observer(MetadataKeyRequestObserver::new("x-api-key".to_string()));

    // Setup cancellation for graceful shutdown
    let cts = CancellationToken::new();
    ctrlc::set_handler({
        let cts = cts.clone();
        move || {
            cts.cancel();
        }
    })?;

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

    node.build()?.start(cts.clone(), args.wait_for_rpc).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        CliCommand::Start(args) => start(args).await,
    }
}
