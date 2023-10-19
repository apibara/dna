use apibara_observability::init_opentelemetry;
use apibara_sink_common::{run_sink_connector, set_ctrlc_handler, OptionsFromCli};
use apibara_sink_postgres::{PostgresSink, SinkPostgresError, SinkPostgresOptions};
use clap::{Args, Parser, Subcommand};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
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
    postgres: SinkPostgresOptions,
    #[command(flatten)]
    common: OptionsFromCli,
}

#[tokio::main]
async fn main() -> Result<(), SinkPostgresError> {
    init_opentelemetry()
        .change_context(SinkPostgresError)
        .attach_printable("failed to initialize opentelemetry")?;
    let args = Cli::parse();

    let ct = CancellationToken::new();
    set_ctrlc_handler(ct.clone())
        .change_context(SinkPostgresError)
        .attach_printable("failed to setup ctrl-c handler")?;

    match args.subcommand {
        Command::Run(args) => {
            run_sink_connector::<PostgresSink>(&args.script, args.common, args.postgres, ct)
                .await
                .change_context(SinkPostgresError)
                .attach_printable("error while running postgres sink")?;
        }
    }

    Ok(())
}
