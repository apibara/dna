use apibara_observability::init_opentelemetry;
use apibara_operator::sink::SinkWebhook;
use clap::{Args, Parser, Subcommand};
use kube::CustomResourceExt;

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
struct StartArgs {}

fn generate_crds(_args: GenerateCrdArgs) -> anyhow::Result<()> {
    let crds = [SinkWebhook::crd()]
        .iter()
        .map(|crd| serde_yaml::to_string(&crd))
        .collect::<Result<Vec<_>, _>>()?
        .join("---\n");
    println!("{}", crds);
    Ok(())
}

async fn start(_args: StartArgs) -> anyhow::Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    match args.command {
        Command::GenerateCrd(args) => generate_crds(args)?,
        Command::Start(args) => start(args).await?,
    }

    Ok(())
}
