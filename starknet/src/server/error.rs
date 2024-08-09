#[derive(Debug)]
pub struct StreamServerError;

impl error_stack::Context for StreamServerError {}

impl std::fmt::Display for StreamServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Starknet stream server error")
    }
}
