use anyhow::{Error, Result};
use futures::StreamExt;
use std::{str::FromStr, time::Duration};
use tokio::task::JoinHandle;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{
    application::{Application, ApplicationId},
    chain::{EventFilter, TopicValue},
    indexer::IndexerConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();
    // setup indexer
    let transfer_topic =
        TopicValue::from_str("0x02CFB12FF9E08412EC5009C65EA06E727119AD948D25C8A8CC2C86FEC4ADEE70")?;
    let filter = EventFilter::empty().add_topic(transfer_topic);
    let indexer_config = IndexerConfig::new(90_000).add_filter(filter);

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
}
