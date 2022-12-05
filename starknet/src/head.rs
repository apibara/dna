//! Head tracker service.
//!
//! This service starts tracking the current chain head on start.

use std::{error::Error, sync::Arc};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::provider::{BlockId, Provider};

pub struct HeadTracker<G: Provider + Send> {
    provider: Arc<G>,
}

#[derive(Debug, thiserror::Error)]
pub enum HeadTrackerError {
    #[error("failed to fetch provider data")]
    Provider(#[from] Box<dyn Error + Send + Sync + 'static>),
}

impl<G> HeadTracker<G>
where
    G: Provider + Send,
{
    pub fn new(provider: Arc<G>) -> Self {
        HeadTracker { provider }
    }

    pub async fn start(self, ct: CancellationToken) -> Result<(), HeadTrackerError> {
        info!("HELLO");
        let latest_block_id = BlockId::Latest;
        let pending_block_id = BlockId::Pending;
        let xxx = self.provider.get_block(&latest_block_id).await.unwrap();
        todo!()
    }
}
