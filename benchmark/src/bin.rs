use apibara_benchmark::{BenchmarkError, Cli};
use apibara_observability::init_opentelemetry;
use clap::Parser;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), BenchmarkError> {
    let args = Cli::parse();
    run_with_args(args).await
}

async fn run_with_args(args: Cli) -> Result<(), BenchmarkError> {
    init_opentelemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .change_context(BenchmarkError)
        .attach_printable("failed to initialize opentelemetry")?;

    let ct = CancellationToken::new();

    ctrlc::set_handler({
        let ct = ct.clone();
        move || {
            info!("SIGINT received");
            ct.cancel();
        }
    })
    .change_context(BenchmarkError)
    .attach_printable("failed to set SIGINT handler")?;

    args.run(ct).await
}
