use thiserror::Error;

/// Errors that can occur during batch committer operations.
#[derive(Error, Debug)]
pub enum BatchCommitterError {
    #[error("namespace not found: {namespace_id}")]
    NamespaceNotFound { namespace_id: String },
    #[error("invalid file reference: {file_ref}")]
    InvalidFileReference { file_ref: String },
    #[error("batch validation failed: {message}")]
    BatchValidation { message: String },
    #[error("commit conflict: {message}")]
    CommitConflict { message: String },
    #[error("storage operation failed: {message}")]
    StorageError { message: String },
    #[error("internal error")]
    Internal,
}

pub type BatchCommitterResult<T> = error_stack::Result<T, BatchCommitterError>;
