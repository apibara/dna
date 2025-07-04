use thiserror::Error;

/// Producer core errors.
#[derive(Error, Debug)]
pub enum BatcherError {
    #[error("parquet writer error")]
    ParquetWriter,
    #[error("arrow error")]
    Arrow,
    #[error("io error")]
    Io,
}

pub type BatcherResult<T> = error_stack::Result<T, BatcherError>;
