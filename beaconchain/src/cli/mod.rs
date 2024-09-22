mod dbg;
mod rpc;
mod start;

use clap::{Parser, Subcommand};
use error_stack::Result;
use start::StartCommand;
use tokio_util::sync::CancellationToken;

use crate::error::BeaconChainError;

use self::dbg::DebugRpcCommand;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the Beaconchain DNA server.
    Start(Box<StartCommand>),
    /// Debug command for the Beacon RPC.
    #[command(name = "dbg-rpc")]
    DebugRpc {
        #[clap(subcommand)]
        command: DebugRpcCommand,
    },
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BeaconChainError> {
        match self.command {
            Command::Start(command) => command.run(ct).await,
            Command::DebugRpc { command } => command.run().await,
        }
    }
}
