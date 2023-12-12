use std::collections::HashMap;

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
    /// Limit the namespace the operator watches.
    #[arg(long, env)]
    pub namespace: Option<String>,
}

#[derive(Args, Debug)]
struct SinkArgs {
    /// Sink type to image mapping.
    ///
    /// Values are separated by commas,
    /// e.g. `console=quay.io/apibara/sink-console:latest,mongo=quay.io/apibara/sink-mongo:latest`.
    #[arg(long, env, value_delimiter = ',')]
    pub sink_images: Option<Vec<String>>,
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
    let configuration = args
        .into_configuration()
        .attach_printable("invalid cli arguments")?;
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
    pub fn into_configuration(self) -> Result<Configuration, OperatorError> {
        let mut configuration = Configuration {
            namespace: self.namespace,
            ..Configuration::default()
        };

        if let Some(sink_images) = self.sink.sink_images {
            let mut sinks = HashMap::new();
            for image_kv in &sink_images {
                match image_kv.split_once('=') {
                    Some((name, image)) if !image.contains('=') => {
                        sinks.insert(
                            name.to_string(),
                            SinkConfiguration {
                                image: image.to_string(),
                            },
                        );
                    }
                    _ => {
                        return Err(OperatorError)
                            .attach_printable_lazy(|| {
                                format!("invalid sink image mapping: {}", image_kv)
                            })
                            .attach_printable("hint: expected format is `type=image`")
                    }
                }
            }

            configuration.with_sinks(sinks);
        }

        Ok(configuration)
    }
}
