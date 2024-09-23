mod dbg;
mod rpc;

use clap::{Parser, Subcommand};
use error_stack::Result;
use tokio_util::sync::CancellationToken;

use crate::error::StarknetError;

use self::dbg::DebugRpcCommand;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the Starknet DNA server.
    Start,
    /// Debug Starknet RPC calls.
    #[command(name = "dbg-rpc")]
    DebugRpc {
        #[clap(subcommand)]
        command: DebugRpcCommand,
    },
}

impl Cli {
    pub async fn run(self, _ct: CancellationToken) -> Result<(), StarknetError> {
        match self.command {
            Command::Start => todo!(),
            Command::DebugRpc { command } => command.run().await,
        }
    }
}
