use apibara_dna_protocol::dna::ingestion;

use crate::{
    core::Cursor,
    ingestion::{IngestionState, Snapshot},
};

#[derive(Debug, Clone)]
pub enum SnapshotChange {
    Started {
        snapshot: Snapshot,
    },
    StateChanged {
        new_state: IngestionState,
        finalized: Cursor,
    },
    BlockIngested {
        cursor: Cursor,
    },
}

impl IngestionState {
    pub fn to_proto(&self) -> ingestion::IngestionState {
        ingestion::IngestionState {
            first_block_number: self.first_block_number,
            group_count: self.group_count,
            extra_segment_count: self.extra_segment_count,
        }
    }

    pub fn from_proto(proto: &ingestion::IngestionState) -> Self {
        IngestionState {
            first_block_number: proto.first_block_number,
            group_count: proto.group_count,
            extra_segment_count: proto.extra_segment_count,
        }
    }
}

impl From<ingestion::IngestionState> for IngestionState {
    fn from(value: ingestion::IngestionState) -> Self {
        IngestionState::from_proto(&value)
    }
}
