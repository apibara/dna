#[derive(Debug, Clone)]
pub struct SegmentOptions {
    /// Segment size, in blocks.
    pub segment_size: usize,
    /// Group size, in segments.
    pub group_size: usize,
    /// Target number of digits in the filename.
    pub target_num_digits: usize,
}

pub trait SegmentExt {
    fn segment_start(&self, options: &SegmentOptions) -> u64;
    fn format_segment_group_name(&self, options: &SegmentOptions) -> String;
    fn format_segment_name(&self, options: &SegmentOptions) -> String;
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

impl SegmentExt for u64 {
    fn segment_start(&self, options: &SegmentOptions) -> u64 {
        self / options.segment_size as u64 * options.segment_size as u64
    }

    fn format_segment_group_name(&self, options: &SegmentOptions) -> String {
        let segment = self.segment_start(options).format_segment_name(options);
        format!("{}-{}", segment, options.group_size)
    }

    fn format_segment_name(&self, options: &SegmentOptions) -> String {
        let start = self.segment_start(options);
        let formatted = format!("{:0>width$}", start, width = options.target_num_digits);
        format!(
            "{}-{}",
            underscore_separated_thousands(&formatted),
            options.segment_size
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
    use super::{SegmentExt, SegmentOptions};

    #[test]
    pub fn test_segment_start() {
        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(0u64.segment_start(&options), 0);
        assert_eq!(99u64.segment_start(&options), 0);
        assert_eq!(100u64.segment_start(&options), 100);
        assert_eq!(101u64.segment_start(&options), 100);
        assert_eq!(1000u64.segment_start(&options), 1000);

        let options = SegmentOptions {
            segment_size: 250,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(0u64.segment_start(&options), 0);
        assert_eq!(100u64.segment_start(&options), 0);
        assert_eq!(250u64.segment_start(&options), 250);
        assert_eq!(499u64.segment_start(&options), 250);
        assert_eq!(500u64.segment_start(&options), 500);
        assert_eq!(749u64.segment_start(&options), 500);
        assert_eq!(750u64.segment_start(&options), 750);
    }

    #[test]
    pub fn test_segment_name() {
        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 6,
            ..Default::default()
        };
        assert_eq!(100u64.format_segment_name(&options), "000_100-100");

        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 9,
            ..Default::default()
        };
        assert_eq!(100100u64.format_segment_name(&options), "000_100_100-100");

        let options = SegmentOptions {
            segment_size: 100,
            target_num_digits: 7,
            ..Default::default()
        };
        assert_eq!(100u64.format_segment_name(&options), "0_000_100-100");
    }
}
