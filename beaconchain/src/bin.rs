use apibara_dna_beaconchain::{cli::Cli, error::BeaconChainError};
use apibara_observability::init_opentelemetry;
use clap::Parser;
use error_stack::{Result, ResultExt};

#[tokio::main]
async fn main() -> Result<(), BeaconChainError> {
    let args = Cli::parse();
    run_with_args(args).await
}

async fn run_with_args(args: Cli) -> Result<(), BeaconChainError> {
    init_opentelemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .change_context(BeaconChainError)
        .attach_printable("failed to initialize opentelemetry")?;

    args.run().await
}
