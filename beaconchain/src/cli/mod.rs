mod dbg;
mod rpc;

use clap::{Parser, Subcommand};
use error_stack::Result;

use crate::error::BeaconChainError;

use self::dbg::{DebugRpcCommand, DebugStoreCommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Debug command for the Beacon RPC.
    #[command(name = "dbg-rpc")]
    DebugRpc {
        #[clap(subcommand)]
        command: DebugRpcCommand,
    },
    /// Debug command for the store.
    #[command(name = "dbg-store")]
    DebugStore {
        #[clap(subcommand)]
        command: DebugStoreCommand,
    },
}

impl Cli {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self.command {
            Command::DebugRpc { command } => command.run().await,
            Command::DebugStore { command } => command.run().await,
        }
    }
}
