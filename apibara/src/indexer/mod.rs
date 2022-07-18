//! Index on-chain data.
mod manager;
mod persistence;
mod service;

pub use manager::IndexerManager;
pub use persistence::{EthereumNetwork, IndexerPersistence, Network, StarkNetNetwork, State};
pub use service::{start_indexer, ClientToIndexerMessage, Message};
