use anyhow::Result;
use apibara_starknet::{start_node, StartArgs};
use clap::{Parser, Subcommand};

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
    match Cli::parse().command {
        CliCommand::Start(args) => start_node(args).await,
    }
}
