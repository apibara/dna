mod ingest;

use clap::{Parser, Subcommand};
use error_stack::Result;
use tokio_util::sync::CancellationToken;

use crate::error::EvmError;

use self::ingest::IngestArgs;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Ingest data from the RPC.
    Ingest(IngestArgs),
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        match self.command {
            Command::Ingest(args) => args.run(ct).await,
        }
    }
}
