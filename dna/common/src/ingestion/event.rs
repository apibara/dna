use apibara_dna_protocol::ingestion;

use crate::segment::SegmentOptions;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub revision: u64,
    pub first_block_number: u64,
    pub segment_options: SegmentOptions,
    pub group_count: u32,
}

#[derive(Debug, Clone)]
pub struct SealGroup {
    pub revision: u64,
    pub first_block_number: u64,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub first_block_number: u64,
}

#[derive(Debug, Clone)]
pub enum SnapshotChange {
    Started(Snapshot),
    GroupSealed(SealGroup),
    SegmentAdded(Segment),
}

impl Snapshot {
    pub fn to_proto(&self) -> ingestion::Snapshot {
        ingestion::Snapshot {
            revision: self.revision,
            first_block_number: self.first_block_number,
            segment_size: self.segment_options.segment_size as u32,
            group_size: self.segment_options.group_size as u32,
            group_count: self.group_count,
        }
    }

    pub fn to_response(&self) -> ingestion::SubscribeResponse {
        ingestion::SubscribeResponse {
            message: Some(ingestion::subscribe_response::Message::Snapshot(
                self.to_proto(),
            )),
        }
    }
}
