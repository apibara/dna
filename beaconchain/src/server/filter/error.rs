#[derive(Debug)]
pub struct SegmentFilterError;

impl error_stack::Context for SegmentFilterError {}

impl std::fmt::Display for SegmentFilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to filter segment data")
    }
}
