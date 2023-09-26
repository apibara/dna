mod paths;
mod plugins;
mod run;
mod test;

use apibara_observability::{init_error_handler, init_opentelemetry};
use apibara_sink_common::apibara_cli_style;
use clap::{Parser, Subcommand};

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
    /// Test an indexer script.
    Test(test::TestArgs),
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    init_error_handler()?;
    init_opentelemetry()?;

    let args = Cli::parse();
    match args.subcommand {
        Command::Run(args) => run::run(args).await,
        Command::Plugins(args) => plugins::run(args).await,
        Command::Test(args) => test::run(args).await,
    }
}
