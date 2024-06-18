use apibara_dna_common::error::Result;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    StartIngestion,
    StartServer,
    Inspect,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        match self.subcommand {
            Command::StartIngestion => todo!(),
            Command::StartServer => todo!(),
            Command::Inspect => todo!(),
        }
    }
}
