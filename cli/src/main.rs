use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use crate::{admin::AdminCommands, bench::BenchArgs, dev::DevArgs, error::CliResult};

mod admin;
mod bench;
mod dev;
mod error;
mod remote;

#[derive(Parser)]
#[command(name = "wings")]
#[command(about = "Wings CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Wings service in development mode
    Dev {
        #[clap(flatten)]
        inner: DevArgs,
    },
    /// Interact with the admin API
    Admin {
        #[command(subcommand)]
        inner: AdminCommands,
    },
    /// Benchmark the Wings HTTP ingestor
    Bench {
        #[clap(flatten)]
        inner: BenchArgs,
    },
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let cli = Cli::parse();

    let ct = CancellationToken::new();

    let ct_clone = ct.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ct_clone.cancel();
    });

    match cli.command {
        Commands::Dev { inner } => inner.run(ct).await,
        Commands::Admin { inner } => inner.run(ct).await,
        Commands::Bench { inner } => inner.run(ct).await,
    }
}
