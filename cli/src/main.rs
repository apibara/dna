use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use starknet_rpc::RpcProvider;
use std::{pin::Pin, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::{
    chain::{BlockEvents, EventFilter, Topic},
    head::{self, BlockStreamMessage},
    indexer::{self, IndexerClient, IndexerConfig},
    starknet::StarkNetChainProvider,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let provider = RpcProvider::new("http://localhost:9545")
        .context("failed to create starknet rpc provider")?;

    let provider = Arc::new(StarkNetChainProvider::new(provider.clone()));

    info!("creating head stream");
    let head_stream = head::start(provider.clone())
        .await
        .context("failed to start head tracker stream")?;

    info!("creating indexer stream");
    let transfer_event =
        Topic::value_from_hex("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
            .context("failed to parse transfer event topic")?;

    let indexer_config = IndexerConfig {
        from_block: 0,
        filters: vec![EventFilter::new().add_topic(transfer_event)],
    };
    let (client, indexer_stream) = indexer::start(provider.clone(), indexer_config);

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
    S: Stream<Item = BlockEvents> + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            info!(
                "indexer {} {}: {}",
                message.number,
                message.hash,
                message.events.len()
            );
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
                }
                BlockStreamMessage::Rollback(head) => {
                    debug!("rollback {} {}", head.number, head.hash);
                    client
                        .update_head(head)
                        .await
                        .context("failed to update head after rollback")
                        .unwrap();
                }
            }
        }
    })
}
