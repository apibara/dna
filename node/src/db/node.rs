//! Node related tables.

use apibara_core::{application::pb, stream::StreamId};

use super::Table;

#[derive(Debug, Clone, Copy, Default)]
pub struct InputStreamTable {}

impl Table for InputStreamTable {
    type Key = StreamId;
    type Value = pb::InputStream;

    fn db_name() -> &'static str {
        "InputStream"
    }
}
