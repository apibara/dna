mod common;
mod ingestion;
mod server;

use apibara_dna_common::error::Result;
use clap::{Parser, Subcommand};
use ingestion::{run_ingestion, StartIngestionArgs};

use self::server::{run_server, StartServerArgs};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    StartIngestion(Box<StartIngestionArgs>),
    StartServer(StartServerArgs),
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.subcommand {
            Command::StartIngestion(args) => run_ingestion(*args).await,
            Command::StartServer(args) => run_server(args).await,
        }
    }
}
