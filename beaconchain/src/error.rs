#[derive(Debug, Clone)]
pub struct BeaconChainError;

impl error_stack::Context for BeaconChainError {}

impl std::fmt::Display for BeaconChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Beacon Chain error")
    }
}
