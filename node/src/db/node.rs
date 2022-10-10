//! Node related tables.

use apibara_core::application::pb;

use super::Table;

/// Contains the node configuration.
#[derive(Clone, prost::Message)]
pub struct Configuration {
    #[prost(message, repeated, tag = "1")]
    pub inputs: ::prost::alloc::vec::Vec<pb::InputStream>,
    #[prost(message, tag = "2")]
    pub output: Option<pb::OutputStream>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConfigurationTable {}

impl Table for ConfigurationTable {
    type Key = ();
    type Value = Configuration;

    fn db_name() -> &'static str {
        "Configuration"
    }
}
