#[derive(Debug)]
pub enum DnaStarknetError {
    Configuration,
    Fatal,
}

impl error_stack::Context for DnaStarknetError {}

impl std::fmt::Display for DnaStarknetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DnaStarknetError::Configuration => write!(f, "DNA Starknet: configuration error"),
            DnaStarknetError::Fatal => write!(f, "DNA Starknet: fatal error"),
        }
    }
}
