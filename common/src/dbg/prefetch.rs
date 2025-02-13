use std::{collections::HashMap, time::Instant};

use error_stack::{Result, ResultExt};
use futures::FutureExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    block_store::BlockStoreReader,
    chain_view::chain_view_sync_loop,
    cli::{EtcdArgs, ObjectStoreArgs},
    data_stream::{DataStreamMetrics, SegmentAccessFetch, SegmentStream},
    file_cache::FileCacheArgs,
    fragment::FragmentId,
    query::BlockFilter,
};

use super::DebugCommandError;

pub async fn run_debug_prefetch_stream(
    filter: BlockFilter,
    fragment_id_to_name: HashMap<FragmentId, String>,
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

    let mut sync_handle = tokio::spawn(chain_view_sync.start(ct.clone()));

    let chain_view = loop {
        if let Some(chain_view) = chain_view.borrow().clone() {
            break chain_view;
        };

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
            _ = ct.cancelled() => {
                return Ok(())
            }
            sync = &mut sync_handle => {
                info!("sync loop terminated");
                sync.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
                return Ok(());
            }
        };
    };

    let starting_cursor = chain_view
        .get_starting_cursor()
        .await
        .change_context(DebugCommandError)?;

    let segment_stream = SegmentStream::new(
        vec![filter],
        fragment_id_to_name,
        block_store,
        chain_view,
        DataStreamMetrics::default(),
    );

    let (tx, rx) = mpsc::channel(queue_size);

    let mut segment_stream_handle =
        tokio::spawn(segment_stream.start(starting_cursor, tx, ct.clone())).fuse();

    let mut consumer_handle = tokio::spawn(run_consumer(rx));

    loop {
        tokio::select! {
            _ = ct.cancelled() => {}
            producer = &mut segment_stream_handle => {
                info!("segment stream handle terminated");
                producer.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
            }
            consumer = &mut consumer_handle => {
                info!("consumer handle terminated");
                consumer.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
                break;
            }
            sync = &mut sync_handle => {
                info!("sync loop terminated");
                sync.change_context(DebugCommandError)?.change_context(DebugCommandError)?;
            }
        }
    }

    Ok(())
}

async fn run_consumer(mut rx: mpsc::Receiver<SegmentAccessFetch>) -> Result<(), DebugCommandError> {
    let mut block_count = 0;
    let time = Instant::now();
    while let Some(segment_fetch) = rx.recv().await {
        let segment_access = segment_fetch
            .wait()
            .await
            .change_context(DebugCommandError)?;
        let elapsed = time.elapsed();

        let iter_start = Instant::now();

        for block in segment_access.iter() {
            let _header = block
                .get_header_fragment()
                .change_context(DebugCommandError)?;
            info!(block_number = block.block_number(), "header");
            block_count += 1;
        }

        info!(
            segment = segment_access.first_block,
            fragment_len = segment_access.fragment_len(),
            blocks = ?segment_access.blocks,
            elapsed = ?elapsed,
            iter_elapsed = ?iter_start.elapsed(),
            block_count,
            "fetched segment"
        );
    }

    Ok(())
}
