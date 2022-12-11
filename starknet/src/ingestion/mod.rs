mod accepted;
mod config;
mod downloader;
mod error;
mod finalized;
mod started;
mod storage;
mod subscription;

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use tokio_util::sync::CancellationToken;

use crate::provider::Provider;

use self::{
    started::StartedBlockIngestion, storage::IngestionStorage,
    subscription::IngestionStreamPublisher,
};

pub use self::{
    config::BlockIngestionConfig,
    error::BlockIngestionError,
    subscription::{IngestionStream, IngestionStreamClient},
};

/// Block ingestion service.
pub struct BlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    storage: IngestionStorage<E>,
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
        let storage = IngestionStorage::new(db);
        let (sub_client, publisher) = IngestionStreamPublisher::new();

        let ingestion = BlockIngestion {
            provider,
            storage,
            config,
            publisher,
        };
        (sub_client, ingestion)
    }

    /// Start ingesting blocks.
    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        StartedBlockIngestion::new(self.provider, self.storage, self.config, self.publisher)
            .start(ct)
            .await
    }
}
