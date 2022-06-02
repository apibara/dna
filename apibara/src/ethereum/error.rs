use thiserror::Error;

/// An error returned when working with the Ethereum providers.
#[derive(Debug, Error)]
pub enum EthereumError {
    #[error("the block is missing the hash")]
    MissingBlockHash,
    #[error("missing block number")]
    MissingBlockNumber,
    #[error("provider error")]
    ProviderError,
}
