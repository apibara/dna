//! Filter on chain events.
use crate::chain::types::{Address, TopicValue};

/// An event topic.
#[derive(Debug, Clone)]
pub enum Topic {
    /// A single value.
    Value(TopicValue),
    /// Choice between multiple values.
    Choice(Vec<TopicValue>),
}

/// Describe how to filter events.
#[derive(Debug, Clone)]
pub struct EventFilter {
    /// Filter by the contract emitting the event.
    pub address: Vec<Address>,
    /// Filter by topics.
    pub topics: Vec<Topic>,
}
