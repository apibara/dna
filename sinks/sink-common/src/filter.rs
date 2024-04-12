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
                let self_always = header.always.unwrap_or(false);
                let other_always = other_header.always.unwrap_or(false);
                header.always = Some(self_always || other_always);
            }
        } else {
            self.header = other.header;
        }
        self.withdrawals.extend(other.withdrawals);
        self.transactions.extend(other.transactions);
        self.logs.extend(other.logs);
    }
}
