mod common;
mod debug;
// mod ingestion;
// mod inspect;
// mod server;

use clap::{Parser, Subcommand};
use error_stack::Result;

use crate::error::DnaEvmError;

use self::debug::{run_debug_chain_tracker, DebugChainTrackerArgs};

// use self::{
//     ingestion::{run_ingestion, StartIngestionArgs},
//     inspect::{run_inspect, InspectArgs},
//     server::{run_server, StartServerArgs},
// };

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    DebugChainTracker(DebugChainTrackerArgs),
    // StartIngestion(StartIngestionArgs),
    // StartServer(StartServerArgs),
    // Inspect(InspectArgs),
}

impl Cli {
    pub async fn run(self) -> Result<(), DnaEvmError> {
        match self.subcommand {
            Command::DebugChainTracker(args) => run_debug_chain_tracker(args).await,
        }
    }
}
