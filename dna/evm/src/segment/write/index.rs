use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
};

use roaring::RoaringBitmap;

use crate::ingestion::models;

#[derive(Default)]
pub struct SegmentIndex {
    pub log_by_address: BTreeMap<models::H160, RoaringBitmap>,
    pub log_by_topic: BTreeMap<models::H256, RoaringBitmap>,
}

impl SegmentIndex {
    pub fn add_logs(&mut self, block_number: u64, receipts: &[models::TransactionReceipt]) {
        for receipt in receipts {
            for log in &receipt.logs {
                self.log_by_address
                    .entry(log.address)
                    .or_default()
                    .insert(block_number as u32);

                if let Some(topic) = log.topics.first() {
                    self.log_by_topic
                        .entry(*topic)
                        .or_default()
                        .insert(block_number as u32);
                }
            }
        }
    }

    pub fn join(&mut self, other: &Self) {
        for (address, bitmap) in &other.log_by_address {
            let entry = self.log_by_address.entry(*address).or_default();
            *entry |= bitmap;
        }

        for (topic, bitmap) in &other.log_by_topic {
            let entry = self.log_by_topic.entry(*topic).or_default();
            *entry |= bitmap;
        }
    }

    pub fn clear(&mut self) {
        self.log_by_address.clear();
        self.log_by_topic.clear();
    }
}

impl Debug for SegmentIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentIndex")
            .field("log_by_address", &self.log_by_address.len())
            .field("log_by_topic", &self.log_by_topic.len())
            .finish()
    }
}
