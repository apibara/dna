use apibara_observability::init_opentelemetry;
use apibara_operator::{
    configuration::{Configuration, SinkWebhookConfiguration},
    controller,
    sink::SinkWebhook,
};
use clap::{Args, Parser, Subcommand};
use color_eyre::eyre::Result;
use kube::{Client, CustomResourceExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate the operator CRDs and exit.
    GenerateCrd(GenerateCrdArgs),
    /// Start the operator.
    Start(StartArgs),
}

#[derive(Args, Debug)]
struct GenerateCrdArgs {}

#[derive(Args, Debug)]
struct StartArgs {
    /// The default image to use for the webhook sink.
    #[arg(long, env)]
    pub sink_webhook_image: Option<String>,
}

fn generate_crds(_args: GenerateCrdArgs) -> Result<()> {
    let crds = [SinkWebhook::crd()]
        .iter()
        .map(|crd| serde_yaml::to_string(&crd))
        .collect::<Result<Vec<_>, _>>()?
        .join("---\n");
    println!("{}", crds);
    Ok(())
}

async fn start(args: StartArgs) -> Result<()> {
    let client = Client::try_default().await?;
    let configuration = args.to_configuration();
    controller::start(client, configuration).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    match args.command {
        Command::GenerateCrd(args) => generate_crds(args)?,
        Command::Start(args) => start(args).await?,
    }

    Ok(())
}

impl StartArgs {
    pub fn to_configuration(&self) -> Configuration {
        let webhook = SinkWebhookConfiguration {
            image: self
                .sink_webhook_image
                .clone()
                .unwrap_or_else(|| "quay.io/apibara/sink-webhook:latest".to_string()),
        };

        Configuration { webhook }
    }
}
