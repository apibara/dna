use anyhow::Result;
use std::str::FromStr;
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
    let indexer_config = IndexerConfig::new(0).add_filter(filter);

    // create application
    let application_id = ApplicationId::new("test-app2")?;
    let application = Application::new(application_id, indexer_config);

    application.run().await?;

    Ok(())
}
