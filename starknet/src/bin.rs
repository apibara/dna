use apibara_node::o11y::init_opentelemetry;
use apibara_starknet::{set_ctrlc_handler, start_node, StarknetError, StartArgs};
use clap::{Parser, Subcommand};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

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
async fn main() -> Result<(), StarknetError> {
    init_opentelemetry()
        .change_context(StarknetError)
        .attach_printable("failed to initialize opentelemetry")?;

    let cts = CancellationToken::new();
    set_ctrlc_handler(cts.clone()).change_context(StarknetError)?;

    match Cli::parse().command {
        CliCommand::Start(args) => start_node(args, cts).await,
    }
}
