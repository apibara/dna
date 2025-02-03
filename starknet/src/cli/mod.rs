mod dbg;
mod rpc;
mod start;

use clap::{Parser, Subcommand};
use dbg::DebugPrefetchCommand;
use error_stack::Result;
use tokio_util::sync::CancellationToken;

use crate::error::StarknetError;

use self::{dbg::DebugRpcCommand, start::StartCommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the Starknet DNA server.
    Start(Box<StartCommand>),
    /// Debug Starknet RPC calls.
    #[command(name = "dbg-rpc")]
    DebugRpc {
        #[clap(subcommand)]
        command: DebugRpcCommand,
    },
    #[command(name = "dbg-prefetch")]
    /// Debug the prefetch module.
    DebugPrefetch(Box<DebugPrefetchCommand>),
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), StarknetError> {
        match self.command {
            Command::Start(command) => command.run(ct).await,
            Command::DebugRpc { command } => command.run().await,
            Command::DebugPrefetch(command) => command.run(ct).await,
        }
    }
}
