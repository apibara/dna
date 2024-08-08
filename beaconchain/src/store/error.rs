#[derive(Debug, Clone)]
pub enum StoreError {
    Indexing,
}

impl error_stack::Context for StoreError {}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Indexing => write!(f, "failed to index data"),
        }
    }
}
