use std::process::ExitCode;

use apibara_sink_common::{
    apibara_cli_style, initialize_sink, run_sink_connector, OptionsFromCli, ReportExt, SinkError,
};
use apibara_sink_console::{ConsoleSink, SinkConsoleOptions};
use clap::{Args, Parser, Subcommand};
use error_stack::Result;
use tokio_util::sync::CancellationToken;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, styles = apibara_cli_style())]
struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
}

#[derive(Args, Debug)]
struct RunArgs {
    /// The path to the indexer script.
    script: String,
    #[command(flatten)]
    console: SinkConsoleOptions,
    #[command(flatten)]
    common: OptionsFromCli,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();
    run_with_args(args).await.to_exit_code()
}

async fn run_with_args(args: Cli) -> Result<(), SinkError> {
    let ct = CancellationToken::new();
    initialize_sink(ct.clone())?;

    match args.subcommand {
        Command::Run(args) => {
            run_sink_connector::<ConsoleSink>(&args.script, args.common, args.console, ct).await
        }
    }
}
