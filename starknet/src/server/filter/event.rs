use apibara_dna_protocol::starknet;

use crate::segment::store;

pub struct EventFilter {
    pub from_address: Option<store::FieldElement>,
    pub keys: Vec<Key>,
    pub strict: bool,
    pub include_reverted: bool,
    pub include_transaction: bool,
    pub include_receipt: bool,
    pub include_messages: bool,
    pub include_siblings: bool,
}

pub enum Key {
    Any,
    Exact(store::FieldElement),
}

impl EventFilter {
    #[allow(clippy::wrong_self_convention)]
    pub fn from_address(&self) -> Option<&store::FieldElement> {
        self.from_address.as_ref()
    }

    pub fn key0(&self) -> Option<&Key> {
        self.keys.first()
    }

    pub fn matches(&self, event: &store::ArchivedEvent) -> bool {
        // If reverted, then we must include reverted events.
        if !self.include_reverted && event.transaction_reverted {
            return false;
        }

        // Address must match.
        if let Some(from_address) = &self.from_address {
            if from_address != &event.from_address {
                return false;
            }
        }

        // If strict, then length must match exactly.
        if self.strict && self.keys.len() != event.keys.len() {
            return false;
        }

        // If not strict, then all keys must be present.
        if self.keys.len() > event.keys.len() {
            return false;
        }

        // Compare event keys.
        for (key, event_key) in self.keys.iter().zip(event.keys.iter()) {
            match key {
                Key::Any => continue,
                Key::Exact(expected) => {
                    if expected != event_key {
                        return false;
                    }
                }
            }
        }

        true
    }
}

impl From<starknet::EventFilter> for EventFilter {
    fn from(value: starknet::EventFilter) -> Self {
        let from_address = value.from_address.map(store::FieldElement::from);
        let keys = value.keys.into_iter().map(Key::from).collect();
        let strict = value.strict.unwrap_or(false);
        let include_reverted = value.include_reverted.unwrap_or(false);
        let include_transaction = value.include_transaction.unwrap_or(false);
        let include_receipt = value.include_receipt.unwrap_or(false);
        let include_messages = value.include_messages.unwrap_or(false);
        let include_siblings = value.include_siblings.unwrap_or(false);

        EventFilter {
            from_address,
            keys,
            strict,
            include_reverted,
            include_transaction,
            include_receipt,
            include_messages,
            include_siblings,
        }
    }
}

impl From<starknet::Key> for Key {
    fn from(key: starknet::Key) -> Self {
        match key.value {
            None => Key::Any,
            Some(value) => Key::Exact(store::FieldElement::from(value)),
        }
    }
}
