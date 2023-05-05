use anyhow::Result;
use apibara_starknet::{start_node, StartArgs};
use clap::{Parser, Subcommand};
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
    Start(StartArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cts = CancellationToken::new();
    match Cli::parse().command {
        CliCommand::Start(args) => start_node(args, cts).await,
    }
}
