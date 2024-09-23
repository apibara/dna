#[derive(Debug, Clone)]
pub enum StoreError {
    Fragment,
    Indexing,
}

impl error_stack::Context for StoreError {}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Fragment => write!(f, "failed to build fragment"),
            StoreError::Indexing => write!(f, "failed to index data"),
        }
    }
}
