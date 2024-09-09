#[derive(Debug)]
pub struct ChainViewError;

impl error_stack::Context for ChainViewError {}

impl std::fmt::Display for ChainViewError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain view error")
    }
}
