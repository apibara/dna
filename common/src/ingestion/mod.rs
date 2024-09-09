mod cli;
mod error;
mod service;
mod state_client;

use apibara_etcd::{EtcdClient, LockOptions};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::object_store::ObjectStore;
use crate::rkyv::Serializable;

pub use self::cli::IngestionArgs;
pub use self::error::{IngestionError, IngestionErrorExt};
pub use self::service::{BlockIngestion, IngestionService, IngestionServiceOptions};

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
