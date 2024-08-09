use apibara_dna_evm::{cli::Cli, error::DnaEvmError};
use apibara_observability::init_opentelemetry;
use clap::Parser;
use error_stack::Result;
use error_stack::ResultExt;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), DnaEvmError> {
    let args = Cli::parse();
    run_with_args(args).await
}

async fn run_with_args(args: Cli) -> Result<(), DnaEvmError> {
    init_opentelemetry()
        .change_context(DnaEvmError::Fatal)
        .attach_printable("failed to initialize opentelemetry")?;

    args.run().await
}
