use std::sync::Arc;

use parquet::errors::ParquetError;
use thiserror::Error;

/// Producer core errors.
#[derive(Error, Debug, Clone)]
pub enum BatcherError {
    #[error("validation error: {message}")]
    Validation { message: &'static str },
    #[error("parquet writer error: {inner}")]
    ParquetWriter { inner: Arc<ParquetError> },
    #[error("arrow error")]
    Arrow,
    #[error("io error")]
    Io,
}

pub type BatcherResult<T> = error_stack::Result<T, BatcherError>;
