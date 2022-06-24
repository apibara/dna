//! Index on-chain data.
mod manager;
mod persistence;
mod service;

pub use manager::IndexerManager;
pub use persistence::{IndexerPersistence, State};
pub use service::{start_indexer, ClientToIndexerMessage, Message};
