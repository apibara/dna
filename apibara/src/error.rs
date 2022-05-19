use thiserror::Error;

use crate::ethereum::EthereumError;

#[derive(Debug, Error)]
pub enum ApibaraError {
    #[error("ethereum provider error")]
    EthereumProviderError(EthereumError),
    #[error("current head block not found")]
    HeadBlockNotFound,
}

pub type Result<T> = std::result::Result<T, ApibaraError>;
