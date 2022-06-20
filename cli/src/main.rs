use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{application::MongoPersistence, configuration::Configuration, server::Server};

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

    let server = Server::new(Arc::new(application_persistence));
    server.serve(server_addr).await?;

    Ok(())
    // setup indexer
    // TopicValue::from_str("0x02CFB12FF9E08412EC5009C65EA06E727119AD948D25C8A8CC2C86FEC4ADEE70")?;
    /*
    let transfer_topic =
        TopicValue::from_str("0x028ECC732E12D910338C3C78B1960BB6E6A8F6746B914696D6EBE15ED46AA241")?;
    let filter = EventFilter::empty().add_topic(transfer_topic);
    let indexer_config = IndexerConfig::new(241_000).add_filter(filter);

    // create application
    let application_id = ApplicationId::new("test-app2")?;
    let application = Application::new(application_id, indexer_config);

    let (application_handle, application_client) = application.start().await?;

    let client_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        info!("starting client loop");
        let rest_time = Duration::from_secs(1);
        loop {
            let state = application_client.application_state().await?;
            if state.is_started() {
                break;
            }
            tokio::time::sleep(rest_time).await;
        }
        info!("application started. start indexing");
        let mut stream = application_client.start_indexing().await?;
        loop {
            tokio::select! {
                block_events = stream.next() => {
                    if let Some(block_events) = block_events {
                        info!("🧨 client got events: {:?}", block_events);
                    }
                }
            }
        }
    });

    tokio::select! {
        res = application_handle => {
            error!("error: {:?}", res);
            Err(Error::msg("application task terminated"))
        }
        res = client_handle => {
            error!("error: {:?}", res);
            Err(Error::msg("client task terminated"))
        }
    }
    */
}
