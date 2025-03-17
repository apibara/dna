mod cli;
mod error;
mod group;
mod group_builder;
mod metrics;
mod prune;
mod segment;
mod segment_builder;
mod service;

use apibara_etcd::{EtcdClient, LockOptions};
use error_stack::{Result, ResultExt};
use metrics::CompactionMetrics;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{chain_view::ChainView, object_store::ObjectStore, options_store::OptionsStore};

pub use self::cli::CompactionArgs;
pub use self::error::CompactionError;
pub use self::service::{CompactionService, CompactionServiceOptions};

pub async fn compaction_service_loop(
    etcd_client: EtcdClient,
    object_store: ObjectStore,
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
    options: CompactionServiceOptions,
    ct: CancellationToken,
) -> Result<(), CompactionError> {
    use apibara_observability::KeyValue;

    let mut lock_client = etcd_client.lock_client(LockOptions::default());

    let metrics = CompactionMetrics::default();

    while !ct.is_cancelled() {
        info!("acquiring compaction lock");

        metrics.up.record(1, &[KeyValue::new("active", false)]);

        let Some(mut lock) = lock_client
            .lock("compaction/lock", ct.clone())
            .await
            .change_context(CompactionError)
            .attach_printable("failed to acquire compaction lock")?
        else {
            warn!("failed to acquire compaction lock");
            break;
        };

        metrics.up.record(1, &[KeyValue::new("active", true)]);

        // Load options from etcd and check if they match the current options.
        let mut options_store = OptionsStore::new(&etcd_client);

        if let Some(segment_size) = options_store
            .get_segment_size()
            .await
            .change_context(CompactionError)
            .attach_printable("failed to get segment size options")?
        {
            if segment_size != options.segment_size {
                return Err(CompactionError)
                    .attach_printable("segment size changed")
                    .attach_printable_lazy(|| format!("stored segment size: {}", segment_size))
                    .attach_printable_lazy(|| {
                        format!("new segment size: {}", options.segment_size)
                    });
            }
        } else {
            options_store
                .set_segment_size(options.segment_size)
                .await
                .change_context(CompactionError)
                .attach_printable("failed to set segment size options")?;
        }

        if let Some(group_size) = options_store
            .get_group_size()
            .await
            .change_context(CompactionError)
            .attach_printable("failed to get group size options")?
        {
            if group_size != options.group_size {
                return Err(CompactionError)
                    .attach_printable("group size changed")
                    .attach_printable_lazy(|| format!("stored group size: {}", group_size))
                    .attach_printable_lazy(|| format!("new group size: {}", options.group_size));
            }
        } else {
            options_store
                .set_group_size(options.group_size)
                .await
                .change_context(CompactionError)
                .attach_printable("failed to set group size options")?;
        }

        let compaction_service = CompactionService::new(
            etcd_client.clone(),
            object_store.clone(),
            chain_view.clone(),
            options.clone(),
            metrics.clone(),
        );

        match compaction_service.start(&mut lock, ct.clone()).await {
            Ok(_) => {
                lock_client
                    .unlock(lock)
                    .await
                    .change_context(CompactionError)?;
                info!("compaction lock released");
                break;
            }
            Err(err) => {
                error!(error = ?err, "compaction service error");
                lock_client
                    .unlock(lock)
                    .await
                    .change_context(CompactionError)?;

                // TODO: configurable with exponential backoff.
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        }
    }

    Ok(())
}
