use std::time::Instant;

use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_store::BlockStoreReader,
    chain_view::{chain_view_sync_loop, ChainView},
    cli::{EtcdArgs, ObjectStoreArgs},
    file_cache::{FileCacheArgs, FileFetch},
    query::BlockFilter,
    Cursor,
};

use super::DebugCommandError;

pub async fn run_debug_prefetch_stream(
    filter: BlockFilter,
    queue_size: usize,
    object_store: ObjectStoreArgs,
    etcd: EtcdArgs,
    cache: FileCacheArgs,
    ct: CancellationToken,
) -> Result<(), DebugCommandError> {
    let object_store = object_store.into_object_store_client().await;
    let file_cache = cache
        .to_file_cache()
        .await
        .change_context(DebugCommandError)?;
    let etcd_client = etcd
        .into_etcd_client()
        .await
        .change_context(DebugCommandError)?;

    let block_store = BlockStoreReader::new(object_store.clone(), file_cache.clone());
    let (chain_view, chain_view_sync) = chain_view_sync_loop(file_cache, etcd_client, object_store)
        .await
        .change_context(DebugCommandError)?;

    let sync_handle = tokio::spawn(chain_view_sync.start(ct.clone()));

    let (tx, rx) = mpsc::channel(queue_size);

    let producer_handle = tokio::spawn(run_producer(
        filter,
        block_store,
        chain_view,
        tx,
        ct.clone(),
    ));

    let consumer_handle = tokio::spawn(run_consumer(rx));

    tokio::select! {
        _ = ct.cancelled() => {}
        producer = producer_handle => {
            info!("producer handle terminated");
            producer.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
        }
        consumer = consumer_handle => {
            info!("consumer handle terminated");
            consumer.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
        }
        sync = sync_handle => {
            info!("sync loop terminated");
            sync.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
        }
    }

    Ok(())
}

async fn run_producer(
    filter: BlockFilter,
    store: BlockStoreReader,
    chain_view: watch::Receiver<Option<ChainView>>,
    tx: mpsc::Sender<FileFetch>,
    ct: CancellationToken,
) -> Result<(), DebugCommandError> {
    let chain_view = loop {
        if let Some(chain_view) = chain_view.borrow().clone() {
            break chain_view;
        };

        info!("waiting for chain view");
        let Some(_) = ct
            .run_until_cancelled(tokio::time::sleep(std::time::Duration::from_secs(1)))
            .await
        else {
            return Ok(());
        };
    };

    let mut current_block = chain_view
        .get_starting_cursor()
        .await
        .change_context(DebugCommandError)?
        .number;
    let group_size = chain_view.get_group_size().await;
    let segment_size = chain_view.get_segment_size().await;
    let blocks_in_group = group_size * segment_size;

    info!(current_block, group_size, segment_size, "starting producer");

    let Some(mut grouped) = chain_view
        .get_grouped_cursor()
        .await
        .change_context(DebugCommandError)?
    else {
        info!("no grouped block");
        return Ok(());
    };

    loop {
        if current_block <= grouped.number {
            info!(block = current_block, "fetching block");

            let file = store.get_segment(&Cursor::new_finalized(current_block), "index");

            let Ok(_) = tx.send(file).await else {
                info!("failed to send block");
                return Ok(());
            };

            current_block += segment_size;
        } else {
            info!("refreshing grouped cursor");
            let Some(new_grouped) = chain_view
                .get_grouped_cursor()
                .await
                .change_context(DebugCommandError)?
            else {
                info!("no grouped block");
                return Ok(());
            };

            if new_grouped != grouped {
                grouped = new_grouped;
            } else {
                info!("waiting for head change");
                let Some(_) = ct.run_until_cancelled(chain_view.head_changed()).await else {
                    info!("chain view terminated");
                    return Ok(());
                };
            }
        }
    }
}

async fn run_consumer(mut rx: mpsc::Receiver<FileFetch>) -> Result<(), DebugCommandError> {
    let mut count = 0;
    let time = Instant::now();
    while let Some(fetch) = rx.recv().await {
        info!(state = ?fetch.state(), "received fetch");
        match fetch.await {
            Ok(segment) => {
                count += 1;
                let elapsed = time.elapsed();
                info!(
                    segment = segment.key(),
                    size = segment.value().len(),
                    elapsed = ?elapsed,
                    count,
                    "fetched segment"
                );
            }
            Err(e) => {
                info!(%e, "failed to fetch segment");
            }
        }
    }

    Ok(())
}
