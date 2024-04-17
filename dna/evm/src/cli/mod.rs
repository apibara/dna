mod common;
mod ingestion;

use apibara_dna_common::error::Result;
use clap::{Parser, Subcommand};

use self::ingestion::{run_ingestion, StartIngestionArgs};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    StartIngestion(StartIngestionArgs),
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.subcommand {
            Command::StartIngestion(args) => run_ingestion(args).await,
        }
    }
}
