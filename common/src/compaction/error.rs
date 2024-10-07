#[derive(Debug)]
pub struct CompactionError;

impl error_stack::Context for CompactionError {}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compaction error")
    }
}
