use apibara_core::{ethereum::v1alpha2::Filter, node::v1alpha2::DataFinality};

use crate::erigon::GlobalBlockId;

pub struct StreamConfiguration {
    pub batch_size: usize,
    pub stream_id: u64,
    pub finality: DataFinality,
    pub starting_cursor: Option<GlobalBlockId>,
    pub filter: Filter,
}
