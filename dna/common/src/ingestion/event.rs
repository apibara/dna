use apibara_dna_protocol::dna::ingestion;
use serde::{Deserialize, Serialize};

use crate::{core::Cursor, segment::SegmentOptions};

/// Ingestion status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot revision.
    pub revision: u64,
    /// Segment options used by this snapshot.
    pub segment_options: SegmentOptions,
    /// Ingestion state.
    pub ingestion: IngestionState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionState {
    /// First block number ingested, inclusive.
    pub first_block_number: u64,
    /// Number of sealed groups in the snapshot.
    pub group_count: u32,
    /// Number of segments not yet sealed.
    pub extra_segment_count: u32,
}

#[derive(Debug, Clone)]
pub struct IngestedBlock {
    pub cursor: Cursor,
}

#[derive(Debug, Clone)]
pub enum SnapshotChange {
    Started(Snapshot),
    StateChanged {
        new_state: IngestionState,
        finalized: Cursor,
    },
    BlockIngested(IngestedBlock),
}

impl Snapshot {
    /// Deserialize a [Snapshot] from the given string.
    pub fn from_str(s: &str) -> Result<Snapshot, serde_json::Error> {
        serde_json::from_str(s)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Returns the first block numbers that should be ingested.
    pub fn starting_block_number(&self) -> u64 {
        let sg_count = self.segment_options.segment_group_blocks();
        let blocks_sealed_count = self.ingestion.group_count as u64 * sg_count;
        let blocks_extra_count =
            self.ingestion.extra_segment_count as u64 * self.segment_options.segment_size as u64;
        self.ingestion.first_block_number + blocks_sealed_count + blocks_extra_count
    }

    pub fn to_proto(&self) -> ingestion::Snapshot {
        ingestion::Snapshot {
            revision: self.revision,
            segment_options: self.segment_options.to_proto().into(),
            ingestion: self.ingestion.to_proto().into(),
        }
    }

    pub fn to_response(&self) -> ingestion::SubscribeResponse {
        ingestion::SubscribeResponse {
            message: Some(ingestion::subscribe_response::Message::Snapshot(
                self.to_proto(),
            )),
        }
    }

    pub fn from_proto(proto: &ingestion::Snapshot) -> Option<Self> {
        let segment_options = proto.segment_options.as_ref()?;
        let ingestion_state = proto.ingestion.as_ref()?;

        Some(Self {
            revision: proto.revision,
            segment_options: SegmentOptions::from_proto(segment_options),
            ingestion: IngestionState::from_proto(ingestion_state),
        })
    }
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

#[cfg(test)]
mod tests {
    use super::{IngestionState, Snapshot};
    use crate::segment::SegmentOptions;

    #[test]
    pub fn test_starting_block_number() {
        let options = SegmentOptions {
            group_size: 25,
            segment_size: 100,
        };

        {
            let s = Snapshot {
                revision: 0,
                segment_options: options.clone(),
                ingestion: IngestionState {
                    first_block_number: 0,
                    group_count: 0,
                    extra_segment_count: 0,
                },
            };

            assert_eq!(s.starting_block_number(), 0);
        }

        {
            let s = Snapshot {
                revision: 0,
                segment_options: options.clone(),
                ingestion: IngestionState {
                    first_block_number: 0,
                    group_count: 0,
                    extra_segment_count: 1,
                },
            };

            assert_eq!(s.starting_block_number(), 100);
        }

        {
            let s = Snapshot {
                revision: 0,
                segment_options: options.clone(),
                ingestion: IngestionState {
                    first_block_number: 5_000_000,
                    group_count: 10,
                    extra_segment_count: 3,
                },
            };

            assert_eq!(s.starting_block_number(), 5_025_300);
        }
    }
}
