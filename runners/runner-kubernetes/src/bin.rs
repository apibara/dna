use std::net::SocketAddr;

use apibara_observability::init_opentelemetry;
use apibara_runner_kubernetes::{
    configuration::Configuration, error::KubeRunnerError, start_server,
};
use clap::{Args, Parser};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub enum Cli {
    /// Start the server.
    Start(StartArgs),
}

#[derive(Args, Debug)]
pub struct StartArgs {
    /// The gRPC server address.
    #[arg(long, env)]
    pub address: Option<String>,
    /// The namespace where to create resources.
    #[arg(long, env)]
    pub target_namespace: String,
}

async fn start(args: StartArgs) -> Result<(), KubeRunnerError> {
    let config = args.into_configuration()?;
    let client = kube::Client::try_default()
        .await
        .change_context(KubeRunnerError)
        .attach_printable("failed to create kubernetes client")?;

    let ct = CancellationToken::new();
    ctrlc::set_handler({
        let ct = ct.clone();
        move || {
            ct.cancel();
        }
    })
    .change_context(KubeRunnerError)
    .attach_printable("failed to setup ctrl-c handler")?;

    start_server(client, config, ct).await
}

#[tokio::main]
async fn main() -> Result<(), KubeRunnerError> {
    init_opentelemetry()
        .change_context(KubeRunnerError)
        .attach_printable("failed to initialize opentelemetry")?;

    let args = Cli::parse();

    match args {
        Cli::Start(args) => start(args).await?,
    }

    Ok(())
}

impl StartArgs {
    pub fn into_configuration(self) -> Result<Configuration, KubeRunnerError> {
        let address = self.address.unwrap_or_else(|| "0.0.0.0:8080".to_string());
        let address = address
            .parse::<SocketAddr>()
            .change_context(KubeRunnerError)
            .attach_printable("failed to parse address")
            .attach_printable_lazy(|| format!("address: {address}"))?;

        if self.target_namespace.is_empty() {
            return Err(KubeRunnerError).attach_printable("target namespace cannot be empty");
        }

        Ok(Configuration {
            address,
            target_namespace: self.target_namespace,
        })
    }
}
