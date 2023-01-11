use std::path::PathBuf;

use anyhow::Result;
use apibara_node::{db::default_data_dir, o11y::init_opentelemetry};
use apibara_starknet::{
    server::{MetadataKeyRequestSpan, SimpleRequestSpan},
    HttpProvider, NoWriteMap, StarkNetNode,
};
use clap::{Args, Parser, Subcommand};
use tokio_util::sync::CancellationToken;

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
}

async fn start(args: StartCommand) -> Result<()> {
    init_opentelemetry()?;

    let mut node = StarkNetNode::<HttpProvider, SimpleRequestSpan, NoWriteMap>::builder(&args.rpc)?
        .with_request_span(MetadataKeyRequestSpan::new("x-api-key".to_string()));

    // give precedence to --data
    if let Some(datadir) = args.data {
        node.with_datadir(datadir);
    } else if let Some(name) = args.name {
        let datadir = default_data_dir()
            .map(|p| p.join(name))
            .expect("no datadir");
        node.with_datadir(datadir);
    }

    // Setup cancellation for graceful shutdown
    let cts = CancellationToken::new();
    ctrlc::set_handler({
        let cts = cts.clone();
        move || {
            cts.cancel();
        }
    })?;

    node.build()?.start(cts.clone()).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        CliCommand::Start(args) => start(args).await,
    }
}
