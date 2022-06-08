//! Index on-chain data.
use crate::chain::EventFilter;

#[derive(Debug)]
/// A single indexer configuration.
pub struct IndexerConfig {
    /// Start indexing from this block.
    pub from_block: u64,
    /// Index these events.
    pub filters: Vec<EventFilter>,
}

impl IndexerConfig {
    pub fn new(from_block: u64) -> Self {
        IndexerConfig {
            from_block,
            filters: Vec::new(),
        }
    }

    pub fn add_filter(mut self, filter: EventFilter) -> Self {
        self.filters.push(filter);
        self
    }
}
