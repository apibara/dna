#[derive(Debug)]
pub struct StarknetError;

impl error_stack::Context for StarknetError {}

impl std::fmt::Display for StarknetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Starknet DNA error")
    }
}
