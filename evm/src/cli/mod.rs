mod dbg;
mod rpc;
mod start;

use apibara_dna_common::dbg::DebugIndexCommand;
use clap::{Parser, Subcommand};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;

use crate::error::EvmError;

use self::{dbg::DebugRpcCommand, start::StartCommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the EVM DNA server.
    Start(Box<StartCommand>),
    /// Debug EVM RPC calls.
    #[command(name = "dbg-rpc")]
    DebugRpc {
        #[clap(subcommand)]
        command: DebugRpcCommand,
    },
    /// Debug the index file.
    #[command(name = "dbg-index")]
    DebugIndex {
        #[clap(subcommand)]
        command: DebugIndexCommand,
    },
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        match self.command {
            Command::Start(command) => command.run(ct).await,
            Command::DebugRpc { command } => command.run().await,
            Command::DebugIndex { command } => command.run().await.change_context(EvmError),
        }
    }
}
