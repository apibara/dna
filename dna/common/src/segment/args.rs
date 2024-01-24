use clap::Args;

use super::SegmentOptions;

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
        if let Some(segment_target_num_digits) = self.segment_target_num_digits {
            options.target_num_digits = segment_target_num_digits;
        }

        options
    }
}
