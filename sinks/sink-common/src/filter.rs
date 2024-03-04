use apibara_dna_protocol::evm;
use prost::Message;
use serde::{de::DeserializeOwned, ser::Serialize};

pub trait Filter: Default + Message + Clone + DeserializeOwned + Serialize {
    /// Merges the given filter into this filter.
    fn merge_filter(&mut self, other: Self);
}

impl Filter for evm::Filter {
    fn merge_filter(&mut self, other: Self) {
        // default to weak headers.
        if let Some(header) = self.header.as_mut() {
            if let Some(other_header) = other.header {
                let self_weak = header.weak.unwrap_or(true);
                let other_weak = other_header.weak.unwrap_or(true);
                header.weak = Some(self_weak || other_weak);
            }
        } else {
            self.header = other.header;
        }
        self.withdrawals.extend(other.withdrawals);
        self.transactions.extend(other.transactions);
        self.logs.extend(other.logs);
    }
}
