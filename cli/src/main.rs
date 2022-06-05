use anyhow::{Context, Result};
use ethers::prelude::{Provider, Ws};
use futures::StreamExt;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{ethereum::EthereumChainProvider, head::HeadTracker};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let provider = Provider::<Ws>::connect("ws://localhost:8546")
        .await
        .context("failed to connect to ws")?;

    let provider = Arc::new(EthereumChainProvider::new(provider));

    let mut head_tracker = HeadTracker::new(provider.clone());

    let mut head_stream = head_tracker
        .start()
        .await
        .context("failed to start head tracker stream")?;

    info!("starting head stream");
    while let Some(message) = head_stream.next().await {
        info!("-> {:?}", message)
    }

    Ok(())
}
