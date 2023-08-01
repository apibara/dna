use apibara_observability::{init_error_handler, init_opentelemetry};
use apibara_sink_common::{run_sink_connector, set_ctrlc_handler, OptionsFromCli};
use apibara_sink_webhook::{SinkWebhookOptions, WebhookSink};
use clap::{Args, Parser, Subcommand};
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
    webhook: SinkWebhookOptions,
    #[command(flatten)]
    common: OptionsFromCli,
}

#[tokio::main]
async fn main() -> color_eyre::eyre::Result<()> {
    init_error_handler()?;
    init_opentelemetry()?;
    let args = Cli::parse();

    let ct = CancellationToken::new();
    set_ctrlc_handler(ct.clone())?;

    match args.subcommand {
        Command::Run(args) => {
            run_sink_connector::<WebhookSink>(&args.script, args.common, args.webhook, ct).await?;
        }
    }

    Ok(())
}
