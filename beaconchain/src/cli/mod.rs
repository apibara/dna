mod dbg;
mod rpc;

use clap::{Parser, Subcommand};
use dbg::DebugChainCommand;
use error_stack::Result;

use crate::error::BeaconChainError;

use self::dbg::{DebugGroupCommand, DebugRpcCommand, DebugSegmentCommand, DebugStoreCommand};

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
    /// Debug command for the Beacon RPC.
    #[command(name = "dbg-chain")]
    DebugChain {
        #[clap(subcommand)]
        command: DebugChainCommand,
    },
    /// Debug utilities for the store.
    #[command(name = "dbg-store")]
    DebugStore {
        #[clap(subcommand)]
        command: DebugStoreCommand,
    },
    /// Debug the content of a segment.
    #[command(name = "dbg-segment")]
    DebugSegment {
        #[clap(subcommand)]
        command: DebugSegmentCommand,
    },
    /// Debug the content of a segment group.
    #[command(name = "dbg-group")]
    DebugGroup {
        #[clap(subcommand)]
        command: DebugGroupCommand,
    },
}

impl Cli {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self.command {
            Command::DebugRpc { command } => command.run().await,
            Command::DebugChain { command } => command.run().await,
            Command::DebugStore { command } => command.run().await,
            Command::DebugSegment { command } => command.run().await,
            Command::DebugGroup { command } => command.run().await,
        }
    }
}
