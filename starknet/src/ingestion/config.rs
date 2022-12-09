//! Block ingestion configuration.
use std::time::Duration;

/// Block ingestion configuration.
#[derive(Debug)]
pub struct BlockIngestionConfig {
    /// Concurrency for RPC requests.
    pub rpc_concurrency: usize,
    /// How often to refresh head block.
    pub head_refresh_interval: Duration,
}

impl Default for BlockIngestionConfig {
    fn default() -> Self {
        BlockIngestionConfig {
            rpc_concurrency: 16,
            head_refresh_interval: Duration::from_secs(3),
        }
    }
}
