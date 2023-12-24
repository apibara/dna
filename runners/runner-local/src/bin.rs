use std::net::SocketAddr;

use apibara_observability::init_opentelemetry;
use apibara_runner_common::error::{RunnerError, RunnerReportExt, RunnerResultExt};
use apibara_runner_local::{configuration::Configuration, start_server};
use clap::{Args, Parser};
use error_stack::Result;

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
}

async fn start(args: StartArgs) -> Result<(), RunnerError> {
    let config = args.into_configuration()?;

    let ct = CancellationToken::new();
    ctrlc::set_handler({
        let ct = ct.clone();
        move || {
            ct.cancel();
        }
    })
    .internal("failed to setup ctrl-c handler")?;

    start_server(config, ct).await
}

#[tokio::main]
async fn main() -> Result<(), RunnerError> {
    init_opentelemetry().map_err(|err| err.internal("failed to initialize opentelemetry"))?;

    let args = Cli::parse();

    match args {
        Cli::Start(args) => start(args).await?,
    }

    Ok(())
}

impl StartArgs {
    pub fn into_configuration(self) -> Result<Configuration, RunnerError> {
        let address = self.address.unwrap_or_else(|| "0.0.0.0:8080".to_string());
        let address = address
            .parse::<SocketAddr>()
            .internal(&format!("failed to parse address: {address}"))?;

        Ok(Configuration { address })
    }
}
