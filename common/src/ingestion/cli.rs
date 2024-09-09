use clap::Args;

#[derive(Args, Debug)]
pub struct IngestionArgs {
    /// Whether to run the ingestion service.
    #[clap(long = "ingestion.enabled", env = "DNA_INGESTION_ENABLED")]
    pub ingestion_enabled: bool,
    /// How many concurrent ingestion tasks to run.
    #[clap(
        long = "ingestion.max-concurrent-tasks",
        env = "DNA_INGESTION_MAX_CONCURRENT_TASKS",
        default_value = "100"
    )]
    pub ingestion_max_concurrent_tasks: usize,
    /// How many blocks each chain segment contains.
    #[clap(
        long = "ingestion.chain-segment-size",
        env = "DNA_INGESTION_CHAIN_SEGMENT_SIZE",
        default_value = "10000"
    )]
    pub ingestion_chain_segment_size: usize,
    /// Override the ingestion starting block.
    #[clap(
        long = "ingestion.dangerously-override-starting-block",
        env = "DNA_INGESTION_DANGEROUSLY_OVERRIDE_STARTING_BLOCK"
    )]
    pub ingestion_dangerously_override_starting_block: Option<u64>,
    /// How often to refresh the head block, in seconds.
    #[clap(
        long = "ingestion.head-refresh-interval",
        env = "DNA_INGESTION_HEAD_REFRESH_INTERVAL",
        default_value = "3"
    )]
    pub ingestion_head_refresh_interval: u64,
    /// How often to refresh the finalized block, in seconds.
    #[clap(
        long = "ingestion.finalized-refresh-interval",
        env = "DNA_INGESTION_FINALIZED_REFRESH_INTERVAL",
        default_value = "30"
    )]
    pub ingestion_finalized_refresh_interval: u64,
}

impl IngestionArgs {
    pub fn to_ingestion_options(&self) -> super::IngestionServiceOptions {
        super::IngestionServiceOptions {
            max_concurrent_tasks: self.ingestion_max_concurrent_tasks,
            chain_segment_size: self.ingestion_chain_segment_size,
            chain_segment_upload_offset_size: 100,
            override_starting_block: self.ingestion_dangerously_override_starting_block,
            head_refresh_interval: std::time::Duration::from_secs(
                self.ingestion_head_refresh_interval,
            ),
            finalized_refresh_interval: std::time::Duration::from_secs(
                self.ingestion_finalized_refresh_interval,
            ),
        }
    }
}
