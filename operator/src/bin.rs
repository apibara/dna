use apibara_observability::init_opentelemetry;
use apibara_operator::{
    configuration::{Configuration, SinkConfiguration},
    controller,
    crd::Indexer,
    error::OperatorError,
};
use clap::{Args, Parser, Subcommand};
use error_stack::{Result, ResultExt};
use kube::{Client, CustomResourceExt};
use tokio_util::sync::CancellationToken;

/// Base container registry path.
static CONTAINER_REGISTRY: &str = "quay.io/apibara";

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
    #[clap(flatten)]
    pub sink: SinkArgs,
}

#[derive(Args, Debug)]
struct SinkArgs {
    /// The default image to use for the console sink.
    #[arg(long, env)]
    pub sink_console_image: Option<String>,
    /// The default image to use for the MongoDB sink.
    #[arg(long, env)]
    pub sink_mongo_image: Option<String>,
    /// The default image to use for the Parquet sink.
    #[arg(long, env)]
    pub sink_parquet_image: Option<String>,
    /// The default image to use for the PostgreSQL sink.
    #[arg(long, env)]
    pub sink_postgres_image: Option<String>,
    /// The default image to use for the webhook sink.
    #[arg(long, env)]
    pub sink_webhook_image: Option<String>,
}

fn generate_crds(_args: GenerateCrdArgs) -> Result<(), OperatorError> {
    let crds = [Indexer::crd()]
        .iter()
        .map(|crd| serde_yaml::to_string(&crd))
        .collect::<std::result::Result<Vec<_>, _>>()
        .change_context(OperatorError)
        .attach_printable("failed to serialize CRD to yaml")?
        .join("---\n");
    println!("{}", crds);
    Ok(())
}

async fn start(args: StartArgs) -> Result<(), OperatorError> {
    let client = Client::try_default()
        .await
        .change_context(OperatorError)
        .attach_printable("failed to build Kubernetes client")?;
    let configuration = args.into_configuration();
    let ct = CancellationToken::new();

    ctrlc::set_handler({
        let ct = ct.clone();
        move || {
            ct.cancel();
        }
    })
    .change_context(OperatorError)
    .attach_printable("failed to setup ctrl-c handler")?;

    controller::start(client, configuration, ct)
        .await
        .change_context(OperatorError)
        .attach_printable("error while running operator")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), OperatorError> {
    init_opentelemetry()
        .change_context(OperatorError)
        .attach_printable("failed to initialize opentelemetry")?;
    let args = Cli::parse();

    match args.command {
        Command::GenerateCrd(args) => generate_crds(args)?,
        Command::Start(args) => start(args).await?,
    }

    Ok(())
}

impl StartArgs {
    pub fn into_configuration(self) -> Configuration {
        let console = to_sink_configuration(self.sink.sink_console_image, "sink-console");
        let mongo = to_sink_configuration(self.sink.sink_mongo_image, "sink-mongo");
        let parquet = to_sink_configuration(self.sink.sink_parquet_image, "sink-parquet");
        let postgres = to_sink_configuration(self.sink.sink_postgres_image, "sink-postgres");
        let webhook = to_sink_configuration(self.sink.sink_webhook_image, "sink-webhook");

        Configuration {
            console,
            mongo,
            parquet,
            postgres,
            webhook,
        }
    }
}

fn to_sink_configuration(image: Option<String>, default_image: &str) -> SinkConfiguration {
    SinkConfiguration {
        image: image.unwrap_or_else(|| format!("{}/{}:latest", CONTAINER_REGISTRY, default_image)),
    }
}
