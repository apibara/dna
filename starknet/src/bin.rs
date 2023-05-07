use anyhow::Result;
use apibara_node::o11y::init_opentelemetry;
use apibara_starknet::{set_ctrlc_handler, start_node, StartArgs};
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
    init_opentelemetry()?;

    let cts = CancellationToken::new();
    set_ctrlc_handler(cts.clone())?;

    match Cli::parse().command {
        CliCommand::Start(args) => start_node(args, cts).await,
    }
}
