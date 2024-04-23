mod common;
mod ingestion;
mod inspect;
mod server;

use apibara_dna_common::error::Result;
use clap::{Parser, Subcommand};

use self::{
    ingestion::{run_ingestion, StartIngestionArgs},
    inspect::{run_inspect, InspectArgs},
    server::{run_server, StartServerArgs},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    StartIngestion(StartIngestionArgs),
    StartServer(StartServerArgs),
    Inspect(InspectArgs),
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.subcommand {
            Command::StartIngestion(args) => run_ingestion(args).await,
            Command::StartServer(args) => run_server(args).await,
            Command::Inspect(args) => run_inspect(args).await,
        }
    }
}
