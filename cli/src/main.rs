use anyhow::{Context, Result};
use ethers::prelude::{Provider, Ws};
use futures::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{
    chain::BlockEvents,
    ethereum::EthereumChainProvider,
    head::{self, BlockStreamMessage},
    indexer::{self, IndexerClient},
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let provider = Provider::<Ws>::connect("ws://localhost:8546")
        .await
        .context("failed to connect to ws")?;

    let provider = Arc::new(EthereumChainProvider::new(provider.clone()));

    info!("creating head stream");
    let head_stream = head::start(provider.clone())
        .await
        .context("failed to start head tracker stream")?;

    info!("creating indexer stream");
    let (client, indexer_stream) = indexer::start(provider.clone(), 0);

    let indexer_stream_handle = run_indexer_stream(Box::pin(indexer_stream));
    let head_stream_handle = run_head_stream(Box::pin(head_stream), client);

    tokio::select! {
        _ = indexer_stream_handle => {
            error!("indexer stream handle exited");
        }
        _ = head_stream_handle => {
            error!("head stream handle exited");
        }
    }
    Ok(())
}

fn run_indexer_stream<S>(mut stream: Pin<Box<S>>) -> JoinHandle<()>
where
    S: Stream<Item = BlockEvents> + Send + Sync + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            info!("indexer {:?}", message);
        }
    })
}

fn run_head_stream<S>(mut stream: Pin<Box<S>>, client: IndexerClient) -> JoinHandle<()>
where
    S: Stream<Item = BlockStreamMessage> + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            match message {
                BlockStreamMessage::NewBlock(head) => {
                    debug!("new block {} {}", head.number, head.hash);
                    client
                        .update_head(head)
                        .await
                        .context("failed to update head after new block")
                        .unwrap();
                    debug!("done");
                }
                BlockStreamMessage::Rollback(head) => {
                    debug!("rollback {} {}", head.number, head.hash);
                    client
                        .update_head(head)
                        .await
                        .context("failed to update head after rollback")
                        .unwrap();
                    debug!("done");
                }
            }
        }
    })
}
