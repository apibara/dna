use prost::Message;
use serde::{de::DeserializeOwned, ser::Serialize};

pub trait Filter: Default + Message + Clone + DeserializeOwned + Serialize {
    /// Merges the given filter into this filter.
    fn merge_filter(&mut self, other: Self);
}
