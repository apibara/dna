mod cli;
mod error;
mod service;
mod state_client;

use apibara_etcd::{EtcdClient, LockOptions};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::object_store::ObjectStore;
use crate::options_store::OptionsStore;
use crate::rkyv::Serializable;

pub use self::cli::IngestionArgs;
pub use self::error::{IngestionError, IngestionErrorExt};
pub use self::service::{BlockIngestion, IngestionService, IngestionServiceOptions};
pub use self::state_client::{
    IngestionStateClient, IngestionStateClientError, FINALIZED_KEY, INGESTED_KEY,
    INGESTION_PREFIX_KEY, STARTING_BLOCK_KEY,
};

pub async fn ingestion_service_loop<B, I>(
    ingestion: I,
    etcd_client: EtcdClient,
    object_store: ObjectStore,
    options: IngestionServiceOptions,
    ct: CancellationToken,
) -> Result<(), IngestionError>
where
    I: BlockIngestion<Block = B> + Send + Sync + 'static,
    B: Send + Sync + 'static + for<'a> Serializable<'a>,
{
    let mut lock_client = etcd_client.lock_client(LockOptions::default());

    while !ct.is_cancelled() {
        info!("acquiring ingestion lock");

        let Some(mut lock) = lock_client
            .lock("ingestion/lock", ct.clone())
            .await
            .change_context(IngestionError::LockKeepAlive)?
        else {
            warn!("failed to acquire ingestion lock");
            break;
        };

        info!("ingestion lock acquired");

        // Compare the current options with the stored options.
        // If they differ, return an error.
        let mut options_store = OptionsStore::new(&etcd_client);
        if let Some(chain_segment_size) = options_store
            .get_chain_segment_size()
            .await
            .change_context(IngestionError::Options)
            .attach_printable("failed to get chain segment size options")?
        {
            if chain_segment_size != options.chain_segment_size {
                return Err(IngestionError::Options)
                    .attach_printable("chain segment size changed")
                    .attach_printable_lazy(|| {
                        format!("stored chain segment size: {}", chain_segment_size)
                    })
                    .attach_printable_lazy(|| {
                        format!("new chain segment size: {}", options.chain_segment_size)
                    });
            }
        } else {
            options_store
                .set_chain_segment_size(options.chain_segment_size)
                .await
                .change_context(IngestionError::Options)
                .attach_printable("failed to set chain segment size options")?;
        }

        let ingestion_service = IngestionService::new(
            ingestion.clone(),
            etcd_client.clone(),
            object_store.clone(),
            options.clone(),
        );

        match ingestion_service.start(&mut lock, ct.clone()).await {
            Ok(_) => {
                lock_client
                    .unlock(lock)
                    .await
                    .change_context(IngestionError::LockKeepAlive)?;
                info!("ingestion lock released");
                break;
            }
            Err(err) => {
                error!(error = ?err, "ingestion service error");
                lock_client
                    .unlock(lock)
                    .await
                    .change_context(IngestionError::LockKeepAlive)?;
                info!("ingestion lock released");

                // TODO: configurable with exponential backoff.
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        }
    }

    Ok(())
}
