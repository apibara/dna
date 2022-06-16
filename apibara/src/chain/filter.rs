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
    /// Filter by the contracts emitting the event.
    pub address: Option<Address>,
    /// Filter by topics.
    pub topics: Vec<Topic>,
}

impl EventFilter {
    /// Create a new (empty) event filter.
    pub fn empty() -> Self {
        EventFilter {
            address: None,
            topics: Vec::new(),
        }
    }

    /// Filter events emitted by this address.
    pub fn with_address(mut self, address: Address) -> Self {
        self.address = Some(address);
        self
    }

    /// Filter events that match this topic.
    pub fn add_topic(mut self, topic: impl Into<Topic>) -> Self {
        self.topics.push(topic.into());
        self
    }
}

impl From<TopicValue> for Topic {
    fn from(value: TopicValue) -> Self {
        Topic::Value(value)
    }
}
