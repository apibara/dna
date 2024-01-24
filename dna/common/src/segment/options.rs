/// Options for creating segments and segment groups.
#[derive(Debug, Clone)]
pub struct SegmentOptions {
    /// Segment size, in blocks.
    pub segment_size: usize,
    /// Group size, in segments.
    pub group_size: usize,
    /// Target number of digits in the filename.
    pub target_num_digits: usize,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            segment_size: 100,
            group_size: 100,
            target_num_digits: 9,
        }
    }
}

impl SegmentOptions {
    /// Compute the first block number of the segment group.
    ///
    /// Notice that we always assume that block numbers start at 0.
    pub fn segment_group_start(&self, block_number: u64) -> u64 {
        block_number / self.segment_size as u64 / self.group_size as u64
            * self.segment_size as u64
            * self.group_size as u64
    }

    /// Compute the first block number of the segment.
    ///
    /// Notice that we always assume that block numbers start at 0.
    pub fn segment_start(&self, block_number: u64) -> u64 {
        block_number / self.segment_size as u64 * self.segment_size as u64
    }

    /// Format the segment group name.
    pub fn format_segment_group_name(&self, block_number: u64) -> String {
        let group_start = self.segment_group_start(block_number);
        let segment = self.format_segment_name(group_start);
        format!("{}-{}", segment, self.group_size)
    }

    /// Format the segment name.
    pub fn format_segment_name(&self, block_number: u64) -> String {
        let start = self.segment_start(block_number);
        let formatted = format!("{:0>width$}", start, width = self.target_num_digits);
        format!(
            "{}-{}",
            underscore_separated_thousands(&formatted),
            self.segment_size
        )
    }
}

fn underscore_separated_thousands(input: &str) -> String {
    let mut output = String::new();
    let mut count = 0;
    for c in input.chars().rev() {
        if count == 3 {
            output.push('_');
            count = 0;
        }
        output.push(c);
        count += 1;
    }
    output.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::SegmentOptions;

    #[test]
    pub fn test_segment_start() {
        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(options.segment_start(0), 0);
        assert_eq!(options.segment_start(99), 0);
        assert_eq!(options.segment_start(100), 100);
        assert_eq!(options.segment_start(101), 100);
        assert_eq!(options.segment_start(1000), 1000);

        let options = SegmentOptions {
            segment_size: 250,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(options.segment_start(0), 0);
        assert_eq!(options.segment_start(100), 0);
        assert_eq!(options.segment_start(250), 250);
        assert_eq!(options.segment_start(499), 250);
        assert_eq!(options.segment_start(500), 500);
        assert_eq!(options.segment_start(749), 500);
        assert_eq!(options.segment_start(750), 750);
    }

    #[test]
    pub fn test_segment_group_start() {
        // 100 blocks per segment
        // 250 segments per group
        let options = SegmentOptions {
            segment_size: 100,
            group_size: 250,
            target_num_digits: 6,
        };

        assert_eq!(options.segment_group_start(0), 0);
        assert_eq!(options.segment_group_start(100), 0);
        assert_eq!(options.segment_group_start(24_999), 0);

        assert_eq!(options.segment_group_start(25_000), 25_000);
        assert_eq!(options.segment_group_start(40_000), 25_000);
        assert_eq!(options.segment_group_start(50_000), 50_000);
    }

    #[test]
    pub fn test_segment_name() {
        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(options.format_segment_name(100), "000_100-100");

        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 9,
            ..Default::default()
        };
        assert_eq!(options.format_segment_name(100_100), "000_100_100-100");

        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 7,
            ..Default::default()
        };
        assert_eq!(options.format_segment_name(100), "0_000_100-100");
    }
}
