#[derive(Debug)]
pub enum DnaEvmError {
    Configuration,
    Fatal,
}

impl error_stack::Context for DnaEvmError {}

impl std::fmt::Display for DnaEvmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DnaEvmError::Configuration => write!(f, "DNA EVM: configuration error"),
            DnaEvmError::Fatal => write!(f, "DNA EVM: fatal error"),
        }
    }
}
