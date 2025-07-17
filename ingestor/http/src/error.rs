use thiserror::Error;

/// Errors that can occur in the HTTP ingestor.
#[derive(Error, Debug)]
pub enum HttpIngestorError {
    #[error("failed to bind to address: {address}")]
    BindError { address: String },
    #[error("server error: {message}")]
    ServerError { message: String },
    #[error("internal error: {0}")]
    Internal(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
}

pub type HttpIngestorResult<T> = error_stack::Result<T, HttpIngestorError>;
