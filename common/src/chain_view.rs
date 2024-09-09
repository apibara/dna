use core::sync;
use std::sync::Arc;

use apibara_etcd::{EtcdClient, KvClient};
use error_stack::{Result, ResultExt};
use futures::TryStreamExt;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    chain::CanonicalChainSegment,
    chain_store::ChainStore,
    ingestion::{FINALIZED_KEY, INGESTED_KEY, INGESTION_PREFIX_KEY, STARTING_BLOCK_KEY},
    object_store::{ObjectETag, ObjectStore},
    Cursor,
};

#[derive(Debug)]
pub struct ChainViewError;

/// Provides a read-only view of the canonical chain.
#[derive(Clone)]
pub struct ChainView(Arc<RwLock<ChainViewInner>>);

pub struct ChainViewSyncService {
    tx: tokio::sync::watch::Sender<Option<ChainView>>,
    etcd_client: EtcdClient,
    chain_store: ChainStore,
}

struct ChainViewInner {
    starting_block: u64,
    finalized: u64,
    recent: CanonicalChainSegment,
}

#[derive(Debug)]
enum IngestionStateUpdate {
    StartingBlock(u64),
    Finalized(u64),
    Ingested(String),
}

impl ChainView {
    pub async fn get_canonical(&self, block_number: u64) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        let cursor = inner
            .recent
            .canonical(block_number)
            .change_context(ChainViewError)
            .attach_printable("failed to get canonical block")?;
        Ok(cursor)
    }

    pub async fn get_starting_cursor(&self) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        let cursor = inner
            .recent
            .canonical(inner.starting_block)
            .change_context(ChainViewError)
            .attach_printable("failed to get starting cursor")?;
        Ok(cursor)
    }

    pub async fn get_finalized_cursor(&self) -> Result<Cursor, ChainViewError> {
        let inner = self.0.read().await;
        let cursor = inner
            .recent
            .canonical(inner.finalized)
            .change_context(ChainViewError)
            .attach_printable("failed to get finalized cursor")?;
        Ok(cursor)
    }

    async fn set_starting_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.starting_block = block;
    }

    async fn set_finalized_block(&self, block: u64) {
        let mut inner = self.0.write().await;
        inner.finalized = block;
    }
}

impl ChainViewSyncService {
    fn new(
        tx: tokio::sync::watch::Sender<Option<ChainView>>,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
    ) -> Self {
        let chain_store = ChainStore::new(object_store);
        Self {
            tx,
            etcd_client,
            chain_store,
        }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), ChainViewError> {
        let mut kv_client = self.etcd_client.kv_client();
        let mut watch_client = self.etcd_client.watch_client();

        let response = kv_client
            .get_prefix(INGESTION_PREFIX_KEY)
            .await
            .change_context(ChainViewError)?;

        let mut starting_block = None;
        let mut finalized = None;

        for kv in response.kvs().iter() {
            let Some(update) = IngestionStateUpdate::from_kv(kv)? else {
                continue;
            };

            match update {
                IngestionStateUpdate::Finalized(block) => {
                    finalized = Some(block);
                }
                IngestionStateUpdate::StartingBlock(block) => {
                    starting_block = Some(block);
                }
                IngestionStateUpdate::Ingested(_) => {}
            }
        }

        let starting_block = starting_block
            .ok_or(ChainViewError)
            .attach_printable("starting block not found")?;
        let finalized = finalized
            .ok_or(ChainViewError)
            .attach_printable("finalized block not found")?;

        let recent_segment = self
            .chain_store
            .get_recent(None)
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get recent canonical chain segment")?
            .ok_or(ChainViewError)
            .attach_printable("recent canonical chain segment not found")?;

        let chain_view = {
            let new_chain_view = ChainViewInner {
                starting_block,
                finalized,
                recent: recent_segment,
            };

            ChainView(Arc::new(RwLock::new(new_chain_view)))
        };

        self.tx
            .send(Some(chain_view.clone()))
            .change_context(ChainViewError)?;

        info!("finished initializing chain view");

        let (_watcher, kv_stream) = watch_client
            .watch_prefix(INGESTION_PREFIX_KEY, ct.clone())
            .await
            .change_context(ChainViewError)?;

        tokio::pin!(kv_stream);

        while let Some(message) = kv_stream.try_next().await.change_context(ChainViewError)? {
            for event in message.events() {
                let Some(kv) = event.kv() else {
                    continue;
                };
                let Some(update) = IngestionStateUpdate::from_kv(kv)? else {
                    continue;
                };

                match update {
                    IngestionStateUpdate::Finalized(block) => {
                        chain_view.set_finalized_block(block).await;
                    }
                    IngestionStateUpdate::StartingBlock(block) => {
                        chain_view.set_starting_block(block).await;
                    }
                    IngestionStateUpdate::Ingested(etag) => {
                        let etag = ObjectETag(etag);
                        // Notice that the ingestion may have uploaded a new segment
                        // So failure here is not a problem.
                        // match self.chain_store.get_recent(etag.into()).await {
                        //     Ok(Some(segment)) => {
                        //         self.cv.set_recent_segment(segment).await?;
                        //     }
                        //     _ => {}
                        // }
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn chain_view_sync_loop(
    etcd_client: EtcdClient,
    object_store: ObjectStore,
) -> Result<
    (
        tokio::sync::watch::Receiver<Option<ChainView>>,
        ChainViewSyncService,
    ),
    ChainViewError,
> {
    let (tx, rx) = tokio::sync::watch::channel(None);

    let sync_service = ChainViewSyncService::new(tx, etcd_client, object_store);

    Ok((rx, sync_service))
}

impl IngestionStateUpdate {
    pub fn from_kv(kv: &etcd_client::KeyValue) -> Result<Option<Self>, ChainViewError> {
        let key = String::from_utf8(kv.key().to_vec())
            .change_context(ChainViewError)
            .attach_printable("failed to decode key")?;

        let value = String::from_utf8(kv.value().to_vec())
            .change_context(ChainViewError)
            .attach_printable("failed to decode value")?;

        if key.ends_with(STARTING_BLOCK_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(ChainViewError)
                .attach_printable("failed to parse starting block")?;
            Ok(Some(IngestionStateUpdate::StartingBlock(block)))
        } else if key.ends_with(FINALIZED_KEY) {
            let block = value
                .parse::<u64>()
                .change_context(ChainViewError)
                .attach_printable("failed to parse finalized block")?;
            Ok(Some(IngestionStateUpdate::Finalized(block)))
        } else if key.ends_with(INGESTED_KEY) {
            Ok(Some(IngestionStateUpdate::Ingested(value)))
        } else {
            Ok(None)
        }
    }
}

impl error_stack::Context for ChainViewError {}

impl std::fmt::Display for ChainViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain view error")
    }
}
