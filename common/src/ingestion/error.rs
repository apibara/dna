use error_stack::Report;

#[derive(Debug, Clone)]
pub enum IngestionError {
    BlockNotFound,
    RpcRequest,
    CanonicalChainStoreRequest,
    BlockStoreRequest,
    StateClientRequest,
    LockKeepAlive,
    BadHash,
    Model,
}

pub trait IngestionErrorExt {
    fn is_block_not_found(&self) -> bool;
}

impl IngestionErrorExt for Report<IngestionError> {
    fn is_block_not_found(&self) -> bool {
        matches!(self.current_context(), IngestionError::BlockNotFound)
    }
}

impl error_stack::Context for IngestionError {}

impl std::fmt::Display for IngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestionError::BlockNotFound => write!(f, "ingestion error: block not found"),
            IngestionError::RpcRequest => write!(f, "ingestion error: rpc request error"),
            IngestionError::BlockStoreRequest => {
                write!(f, "ingestion error: block store request error")
            }
            IngestionError::CanonicalChainStoreRequest => {
                write!(f, "ingestion error: canonical chain store request error")
            }
            IngestionError::StateClientRequest => {
                write!(f, "ingestion error: state client request error")
            }
            IngestionError::BadHash => write!(f, "ingestion error: bad hash"),
            IngestionError::Model => write!(f, "ingestion error: conversion error"),
            IngestionError::LockKeepAlive => {
                write!(f, "ingestion error: failed to keep lock alive")
            }
        }
    }
}
