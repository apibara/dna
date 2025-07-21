use error_stack::Result;
use thiserror::Error;

/// Ingestor error types.
///
/// The message associated with an error is forwarded to the client,
/// for this reason it should contain information that is useful to the user.
#[derive(Error, Debug, Clone)]
pub enum IngestorError {
    /// Internal server error.
    ///
    /// This errors are used when something goes wrong internally.
    #[error("internal server error: {0}")]
    Internal(String),
    /// Schema error.
    ///
    /// This is for errors related to the topic's schema.
    #[error("schema error: {0}")]
    Schema(String),
    /// Validation error.
    ///
    /// This errors are used when a precondition is not met.
    #[error("validation error: {0}")]
    Validation(String),
}

pub type IngestorResult<T> = Result<T, IngestorError>;

impl IngestorError {
    /// Returns the user-visible error message.
    pub fn message(&self) -> String {
        self.to_string()
    }
}
