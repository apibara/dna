use anyhow::{Context, Error, Result};
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{configuration::Configuration, persistence::MongoPersistence, server::Server};

/// CLI to manage an Apibara server.
#[derive(Debug, Parser)]
pub struct Args {
    /// Specify an alternate configuration file.
    #[clap(short, long, global = true)]
    config: Option<String>,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Subcommand)]
pub enum Action {
    /// Start Apibara server.
    Start,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    let configuration = match args.config {
        None => Configuration::default(),
        Some(path) => {
            let path: PathBuf = path.parse().context("failed to parse configuration path")?;
            Configuration::from_path(&path).context("failed to load configuration")?
        }
    };

    let server_addr: SocketAddr = configuration
        .server
        .address
        .parse()
        .context("failed to parse server address")?;

    let application_persistence =
        MongoPersistence::new_with_uri(configuration.admin.storage.connection_string).await?;

    if configuration.network.is_empty() {
        return Err(Error::msg("must register at least one network"));
    }
    let server = Server::new(configuration.network, Arc::new(application_persistence));
    server.serve(server_addr).await?;

    Ok(())
}
