use std::sync::Arc;

use anyhow::{Context, Result};
use ethers::prelude::{Provider, Ws};
use futures::StreamExt;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::ethereum::EthereumBlockHeaderProvider;
use apibara::HeadTracker;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let provider = Provider::<Ws>::connect("ws://localhost:8546")
        .await
        .context("failed to create ethereum provider")?;
    let provider = Arc::new(provider);
    let block_header_provider = EthereumBlockHeaderProvider::new(provider);

    let mut head_tracker = HeadTracker::new(block_header_provider);

    println!("Started head tracker");

    let mut head_stream = head_tracker.start().await?;

    while let Some(message) = head_stream.next().await {
        println!("message = {:?}", message);
    }

    Ok(())
}
