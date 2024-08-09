#[derive(Debug, Clone)]
pub enum IngestionError {
    Api,
    Serialization,
    Storage,
}

impl error_stack::Context for IngestionError {}

impl std::fmt::Display for IngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestionError::Api => write!(f, "Beacon Chain API error"),
            IngestionError::Serialization => write!(f, "Serialization error"),
            IngestionError::Storage => write!(f, "Storage error"),
        }
    }
}
