//! Block ingestion configuration.
use std::time::Duration;

/// Block ingestion configuration.
#[derive(Debug, Clone)]
pub struct BlockIngestionConfig {
    /// Concurrency for RPC requests.
    pub rpc_concurrency: usize,
    /// How often to refresh head block.
    pub head_refresh_interval: Duration,
    /// Override ingestion starting block.
    pub ingestion_starting_block: Option<u64>,
}

impl Default for BlockIngestionConfig {
    fn default() -> Self {
        BlockIngestionConfig {
            rpc_concurrency: 16,
            head_refresh_interval: Duration::from_secs(3),
            ingestion_starting_block: None,
        }
    }
}
