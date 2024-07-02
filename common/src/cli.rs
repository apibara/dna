use clap::Args;

use crate::segment::SegmentOptions;

#[derive(Args, Debug, Clone)]
pub struct IngestionArgs {
    /// Start ingesting data from this block, replacing any existing snapshot.
    #[arg(long, env)]
    pub starting_block: Option<u64>,
    #[clap(flatten)]
    pub segment: SegmentArgs,
}

#[derive(Debug, Args, Clone)]
pub struct SegmentArgs {
    /// Segment size, in blocks.
    #[arg(long, env)]
    pub segment_size: Option<usize>,
    /// Group size, in segments.
    #[arg(long, env)]
    pub segment_group_size: Option<usize>,
    /// Target number of digits in the filename.
    #[arg(long, env)]
    pub segment_target_num_digits: Option<usize>,
}

impl SegmentArgs {
    /// Convert the segment arguments into segment options.
    pub fn to_segment_options(&self) -> SegmentOptions {
        let mut options = SegmentOptions::default();

        if let Some(segment_size) = self.segment_size {
            options.segment_size = segment_size;
        }
        if let Some(segment_group_size) = self.segment_group_size {
            options.group_size = segment_group_size;
        }

        options
    }
}
