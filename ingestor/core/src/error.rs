use error_stack::Result;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum IngestorError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("unsupported arrow schema")]
    UnsupportedArrowSchema,
    #[error("arrow schema mismatch")]
    ArrowSchemaMismatch,
    #[error("parquet write error")]
    ParquetWriteError,
    #[error("batch validation error: {message}")]
    BatchValidationError { message: String },
}

pub type IngestorResult<T> = Result<T, IngestorError>;

impl IngestorError {
    /// Returns the user-visible error message.
    pub fn message(&self) -> String {
        self.to_string()
    }
}
