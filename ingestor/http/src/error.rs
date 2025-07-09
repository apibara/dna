use thiserror::Error;

/// Errors that can occur in the HTTP ingestor.
#[derive(Error, Debug)]
pub enum HttpIngestorError {
    #[error("failed to bind to address: {address}")]
    BindError { address: String },
    #[error("server error: {message}")]
    ServerError { message: String },
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("internal error: {message}")]
    Internal { message: String },
}

pub type HttpIngestorResult<T> = error_stack::Result<T, HttpIngestorError>;
