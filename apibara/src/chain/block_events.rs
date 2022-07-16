use std::collections::HashMap;

use super::types::EventsWithBlockNumberHash;
use crate::chain::{BlockHash, Event};
use anyhow::{Error, Result};

/// Aggregate events and group them by block.
#[derive(Debug, Default)]
pub struct BlockEventsBuilder {
    hashes: HashMap<u64, BlockHash>,
    events: HashMap<u64, Vec<Event>>,
}

impl BlockEventsBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    /// Add `event` to the events in that block.
    pub fn add_event(&mut self, block_number: u64, block_hash: BlockHash, event: Event) {
        let events_in_block = self.events.entry(block_number).or_default();
        events_in_block.push(event);
        self.hashes.entry(block_number).or_insert(block_hash);
    }

    /// Return a vector of `BlockEvents` with the accumulated events.
    pub fn build(self) -> Result<Vec<EventsWithBlockNumberHash>> {
        let mut result = Vec::new();
        for (block_number, mut events) in self.events {
            let block_hash = self
                .hashes
                .get(&block_number)
                .ok_or_else(|| Error::msg("block missing hash"))?
                .to_owned();

            events.sort_by_key(|a| a.log_index());

            let block_with_events = EventsWithBlockNumberHash {
                number: block_number,
                hash: block_hash,
                events,
            };
            result.push(block_with_events);
        }

        result.sort_by(|a, b| a.number.cmp(&b.number));

        Ok(result)
    }
}
