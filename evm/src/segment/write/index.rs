use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Formatter},
};

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use roaring::RoaringBitmap;

use crate::{ingestion::models, segment::store};

#[derive(Default)]
pub struct SegmentIndex {
    pub log_by_address: BTreeMap<models::Address, RoaringBitmap>,
    pub log_by_topic: BTreeMap<models::B256, RoaringBitmap>,
}

impl SegmentIndex {
    pub fn add_logs_sad_face<'a>(
        &mut self,
        block_number: u64,
        logs: impl Iterator<Item = store::Log<'a>>,
    ) {
        for log in logs {
            if let Some(address) = log.address() {
                let address = address.into();
                self.log_by_address
                    .entry(address)
                    .or_default()
                    .insert(block_number as u32);
            }

            if let Some(topic) = log.topics().unwrap_or_default().iter().next() {
                let topic = topic.into();
                self.log_by_topic
                    .entry(topic)
                    .or_default()
                    .insert(block_number as u32);
            }
        }
    }

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

pub trait FlatBufferBuilderSegmentIndexExt<'fbb> {
    fn create_address_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::Address, RoaringBitmap>,
    ) -> Result<WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::AddressBitmapItem<'fbb>>>>>;

    fn create_topic_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::B256, RoaringBitmap>,
    ) -> Result<WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::TopicBitmapItem<'fbb>>>>>;
}

impl<'fbb> FlatBufferBuilderSegmentIndexExt<'fbb> for FlatBufferBuilder<'fbb> {
    fn create_address_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::Address, RoaringBitmap>,
    ) -> Result<WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::AddressBitmapItem<'fbb>>>>>
    {
        let mut items = Vec::new();
        for (key, bitmap) in table {
            let mut buf = Vec::with_capacity(bitmap.serialized_size());
            bitmap
                .serialize_into(&mut buf)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to serialize bitmap")?;
            let bitmap = self.create_vector(&buf);

            let key: store::Address = key.into();
            let mut value = store::AddressBitmapItemBuilder::new(self);
            value.add_key(&key);
            value.add_bitmap(bitmap);

            let value = value.finish();
            items.push(value);
        }
        Ok(self.create_vector(&items))
    }

    fn create_topic_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::B256, RoaringBitmap>,
    ) -> Result<WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::TopicBitmapItem<'fbb>>>>>
    {
        let mut items = Vec::new();
        for (key, bitmap) in table {
            let mut buf = Vec::with_capacity(bitmap.serialized_size());
            bitmap
                .serialize_into(&mut buf)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to serialize bitmap")?;
            let bitmap = self.create_vector(&buf);

            let key: store::B256 = key.into();
            let mut value = store::TopicBitmapItemBuilder::new(self);
            value.add_key(&key);
            value.add_bitmap(bitmap);

            let value = value.finish();
            items.push(value);
        }
        Ok(self.create_vector(&items))
    }
}
