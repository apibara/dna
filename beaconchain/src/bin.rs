use std::process::ExitCode;

use apibara_dna_beaconchain::cli::Cli;
use apibara_dna_common::error::{DnaError, ReportExt, Result};
use apibara_observability::init_opentelemetry;
use clap::Parser;
use error_stack::ResultExt;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();
    run_with_args(args).await.to_exit_code()
}

async fn run_with_args(args: Cli) -> Result<()> {
    init_opentelemetry()
        .change_context(DnaError::Fatal)
        .attach_printable("failed to initialize opentelemetry")?;

    args.run().await
}
