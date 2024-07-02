use apibara_dna_starknet::{cli::Cli, error::DnaStarknetError};
use apibara_observability::init_opentelemetry;
use clap::Parser;
use error_stack::{Result, ResultExt};

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), DnaStarknetError> {
    let args = Cli::parse();
    run_with_args(args).await
}

async fn run_with_args(args: Cli) -> Result<(), DnaStarknetError> {
    init_opentelemetry()
        .change_context(DnaStarknetError::Fatal)
        .attach_printable("failed to initialize opentelemetry")?;

    args.run().await
}
