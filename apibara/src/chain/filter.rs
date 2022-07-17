//! Filter on chain events.
use serde::{Deserialize, Serialize};

use crate::chain::types::Address;

/// Describe how to filter events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFilter {
    /// Filter by the contracts emitting the event.
    pub address: Option<Address>,
    /// Filter by event signature.
    pub signature: String,
}

impl EventFilter {
    /// Create a new (empty) event filter.
    pub fn new_with_signature(signature: impl AsRef<str>) -> Self {
        EventFilter {
            address: None,
            signature: signature.as_ref().to_string(),
        }
    }

    /// Filter events emitted by this address.
    pub fn with_address(mut self, address: Address) -> Self {
        self.address = Some(address);
        self
    }
}
