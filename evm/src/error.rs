#[derive(Debug)]
pub struct EvmError;

impl error_stack::Context for EvmError {}

impl std::fmt::Display for EvmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EVM DNA error")
    }
}
