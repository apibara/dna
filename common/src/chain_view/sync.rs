use std::time::Duration;

use apibara_etcd::EtcdClient;
use error_stack::{Result, ResultExt};
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    chain_store::ChainStore,
    file_cache::FileCache,
    ingestion::{IngestionStateClient, IngestionStateUpdate},
    object_store::ObjectStore,
    options_store::OptionsStore,
};

use super::{error::ChainViewError, full::FullCanonicalChain, view::ChainView};

pub struct ChainViewSyncService {
    tx: tokio::sync::watch::Sender<Option<ChainView>>,
    etcd_client: EtcdClient,
    chain_store: ChainStore,
}

impl ChainViewSyncService {
    fn new(
        tx: tokio::sync::watch::Sender<Option<ChainView>>,
        chain_file_cache: FileCache,
        etcd_client: EtcdClient,
        object_store: ObjectStore,
    ) -> Self {
        let chain_store = ChainStore::new(object_store, chain_file_cache);
        Self {
            tx,
            etcd_client,
            chain_store,
        }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), ChainViewError> {
        info!("starting chain view sync service");
        let mut ingestion_state_client = IngestionStateClient::new(&self.etcd_client);

        let starting_block = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let starting_block = ingestion_state_client
                .get_starting_block()
                .await
                .change_context(ChainViewError)?;

            if let Some(starting_block) = starting_block {
                break starting_block;
            }

            info!(step = "starting_block", "waiting for ingestion to start");
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        let finalized = loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let finalized = ingestion_state_client
                .get_finalized()
                .await
                .change_context(ChainViewError)?;

            if let Some(finalized) = finalized {
                break finalized;
            }

            info!(step = "finalized_block", "waiting for ingestion to start");
            tokio::time::sleep(Duration::from_secs(10)).await;
        };

        let segmented = ingestion_state_client
            .get_segmented()
            .await
            .change_context(ChainViewError)?;

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            let recent = ingestion_state_client
                .get_ingested()
                .await
                .change_context(ChainViewError)?;

            if recent.is_some() {
                break;
            }

            info!(step = "recent", "waiting for ingestion to start");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        if ct.is_cancelled() {
            return Ok(());
        }

        let mut options_store = OptionsStore::new(&self.etcd_client);
        let chain_segment_size = options_store
            .get_chain_segment_size()
            .await
            .change_context(ChainViewError)
            .attach_printable("failed to get chain segment size options")?
            .ok_or(ChainViewError)
            .attach_printable("chain segment size option not found")?;

        let canonical_chain = FullCanonicalChain::initialize(
            self.chain_store.clone(),
            starting_block,
            chain_segment_size,
        )
        .await?;

        let chain_view = ChainView::new(finalized, segmented, canonical_chain);

        self.tx
            .send(Some(chain_view.clone()))
            .change_context(ChainViewError)?;

        info!("finished initializing chain view");

        if ct.is_cancelled() {
            return Ok(());
        }

        let state_changes = ingestion_state_client
            .watch_changes(ct.clone())
            .await
            .change_context(ChainViewError)?;

        tokio::pin!(state_changes);

        while let Some(update) = state_changes
            .try_next()
            .await
            .change_context(ChainViewError)?
        {
            info!(update = ?update, "chain view sync update");
            match update {
                IngestionStateUpdate::StartingBlock(block) => {
                    // The starting block should never be updated.
                    warn!(starting_block = block, "chain view starting block updated");
                }
                IngestionStateUpdate::Finalized(block) => {
                    chain_view.set_finalized_block(block).await;
                }
                IngestionStateUpdate::Segmented(block) => {
                    chain_view.set_segmented_block(block).await;
                }
                IngestionStateUpdate::Ingested(_etag) => {
                    chain_view.refresh_recent().await?;
                }
            }

            self.tx
                .send(Some(chain_view.clone()))
                .change_context(ChainViewError)?;
        }

        if ct.is_cancelled() {
            return Ok(());
        }

        Err(ChainViewError).attach_printable("etcd sync stream ended unexpectedly")
    }
}

pub async fn chain_view_sync_loop(
    chain_file_cache: FileCache,
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

    let sync_service = ChainViewSyncService::new(tx, chain_file_cache, etcd_client, object_store);

    Ok((rx, sync_service))
}
