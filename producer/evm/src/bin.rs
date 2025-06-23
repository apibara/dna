use clap::Parser;
use error_stack::Result;
use wings_producer_evm::{cli::Cli, error::EvmError};

use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), EvmError> {
    let ct = CancellationToken::new();
    let args = Cli::parse();

    args.run(ct).await
}
