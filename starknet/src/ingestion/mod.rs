mod accepted;
mod config;
mod downloader;
mod error;
mod finalized;
mod started;
mod subscription;

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{db::DatabaseStorage, provider::Provider};

use self::{started::StartedBlockIngestion, subscription::IngestionStreamPublisher};

pub use self::{
    config::BlockIngestionConfig,
    error::BlockIngestionError,
    subscription::{IngestionStream, IngestionStreamClient},
};

/// Block ingestion service.
pub struct BlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    db: Arc<Environment<E>>,
    publisher: IngestionStreamPublisher,
}

impl<G, E> BlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(
        provider: Arc<G>,
        db: Arc<Environment<E>>,
        config: BlockIngestionConfig,
    ) -> (IngestionStreamClient, Self) {
        let (sub_client, publisher) = IngestionStreamPublisher::new();

        let ingestion = BlockIngestion {
            provider,
            db,
            config,
            publisher,
        };
        (sub_client, ingestion)
    }

    /// Start ingesting blocks.
    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        loop {
            let storage = DatabaseStorage::new(self.db.clone());
            let result = StartedBlockIngestion::new(
                self.provider.clone(),
                storage,
                self.config.clone(),
                self.publisher.clone(),
            )
            .start(ct.clone())
            .await;

            match result {
                Ok(_) => {
                    if !ct.is_cancelled() {
                        error!("block ingestion stopped without error");
                    }
                    return Ok(());
                }
                Err(err) => {
                    error!(error = ?err, "block ingestion terminated with error");
                }
            }

            // TODO: would be better if we exponentially backed off.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}
