use clap::Args;

#[derive(Args, Debug)]
pub struct CompactionArgs {
    /// Whether to run the compaction service.
    #[clap(long = "compaction.enabled", env = "DNA_COMPACTION_ENABLED")]
    pub compaction_enabled: bool,
    /// How many blocks in a single segment.
    #[clap(
        long = "compaction.segment-size",
        env = "DNA_COMPACTION_SEGMENT_SIZE",
        default_value = "10000"
    )]
    pub compaction_segment_size: usize,
}

impl CompactionArgs {
    pub fn to_compaction_options(&self) -> super::CompactionServiceOptions {
        super::CompactionServiceOptions {
            segment_size: self.compaction_segment_size,
        }
    }
}
