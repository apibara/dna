mod error;
mod paths;
mod plugins;
mod run;
// mod test;

use apibara_observability::init_opentelemetry;
use apibara_sink_common::apibara_cli_style;
use clap::{Parser, Subcommand};
use error::CliError;
use error_stack::{Result, ResultExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, styles = apibara_cli_style())]
struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run an indexer script.
    Run(run::RunArgs),
    /// Manage plugins.
    ///
    /// Plugins are used to extend Apibara functionality, for example by adding new data sinks.
    #[clap(alias = "plugin")]
    Plugins(plugins::PluginsArgs),
    /*
    // Test an indexer script.
    Test(test::TestArgs),
    */
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    init_opentelemetry()
        .change_context(CliError)
        .attach_printable("failed to initialize opentelemetry")?;

    let args = Cli::parse();
    match args.subcommand {
        Command::Run(args) => run::run(args).await,
        Command::Plugins(args) => plugins::run(args).await,
        // Command::Test(args) => test::run(args).await,
    }
}
