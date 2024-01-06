use prost::Message;
use serde::de::DeserializeOwned;

pub trait Filter: Default + Message + Clone + DeserializeOwned {
    /// Merges the given filter into this filter.
    fn merge_filter(&mut self, other: Self);
}
