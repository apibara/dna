use thiserror::Error;

/// CLI error types.
#[derive(Error, Debug)]
pub enum CliError {
    #[error("invalid configuration: {message}")]
    InvalidConfiguration { message: String },
    #[error("service error: {message}")]
    Service { message: String },
    #[error("admin API error: {message}")]
    AdminApi { message: String },
    #[error("object store error: {message}")]
    ObjectStore { message: String },
    #[error("remote API error")]
    Remote,
    #[error("server error: {message}")]
    Server { message: String },
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("I/O error")]
    IoError,
    #[error("invalid JSON")]
    InvalidJson,
}

pub type CliResult<T> = error_stack::Result<T, CliError>;
