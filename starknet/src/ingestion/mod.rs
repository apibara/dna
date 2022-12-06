mod chain;
mod downloader;
mod error;
mod started;
mod storage;

use std::sync::Arc;

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use tokio_util::sync::CancellationToken;

use crate::provider::Provider;

use self::{started::StartedBlockIngestion, storage::IngestionStorage};

pub use self::error::BlockIngestionError;

static RECEIPT_CONCURRENCY: usize = 8;

/// Block ingestion service.
pub struct BlockIngestion<G: Provider + Send, E: EnvironmentKind> {
    provider: Arc<G>,
    storage: IngestionStorage<E>,
}

impl<G, E> BlockIngestion<G, E>
where
    G: Provider + Send,
    E: EnvironmentKind,
{
    pub fn new(provider: Arc<G>, db: Arc<Environment<E>>) -> Self {
        let storage = IngestionStorage::new(db);
        BlockIngestion { provider, storage }
    }

    /// Start ingesting blocks.
    pub async fn start(self, ct: CancellationToken) -> Result<(), BlockIngestionError> {
        let ingestion =
            StartedBlockIngestion::new(self.provider, self.storage, RECEIPT_CONCURRENCY);
        ingestion.start(ct).await
    }
}
