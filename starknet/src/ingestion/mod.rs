mod accepted;
mod config;
mod downloader;
mod error;
mod finalized;
mod started;
mod storage;

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use tokio_util::sync::CancellationToken;

use crate::provider::Provider;

use self::{started::StartedBlockIngestion, storage::IngestionStorage};

pub use self::{config::BlockIngestionConfig, error::BlockIngestionError};

/// Block ingestion service.
pub struct BlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    config: BlockIngestionConfig,
    provider: Arc<G>,
    storage: IngestionStorage<E>,
}

impl<G, E> BlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(provider: Arc<G>, db: Arc<Environment<E>>, config: BlockIngestionConfig) -> Self {
        let storage = IngestionStorage::new(db);
        BlockIngestion {
            provider,
            storage,
            config,
        }
    }

    /// Start ingesting blocks.
    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        StartedBlockIngestion::new(self.provider, self.storage, self.config)
            .start(ct)
            .await
    }
}
