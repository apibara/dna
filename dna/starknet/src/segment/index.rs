use std::{collections::BTreeMap, fmt::Debug};

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, ForwardsUOffset, WIPOffset};
use roaring::RoaringBitmap;

use crate::provider::models;

use super::store;

pub trait FlatBufferBuilderFieldElementBitmapExt<'fbb> {
    fn create_field_element_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::FieldElement, RoaringBitmap>,
    ) -> Result<
        WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::FieldElementBitmap<'fbb>>>>,
    >;
}

#[derive(Default)]
pub struct SegmentIndex {
    pub event_by_address: BTreeMap<models::FieldElement, RoaringBitmap>,
    pub event_by_key: BTreeMap<models::FieldElement, RoaringBitmap>,
}

impl SegmentIndex {
    pub fn add_block_events(
        &mut self,
        block_number: u64,
        receipts: &[models::TransactionReceipt],
    ) -> Result<()> {
        for receipt in receipts {
            use models::TransactionReceipt::*;
            let tx_events = match receipt {
                Invoke(receipt) => receipt.events.iter(),
                L1Handler(receipt) => receipt.events.iter(),
                Declare(receipt) => receipt.events.iter(),
                Deploy(receipt) => receipt.events.iter(),
                DeployAccount(receipt) => receipt.events.iter(),
            };

            for tx_event in tx_events {
                let address = tx_event.from_address;

                self.event_by_address
                    .entry(address)
                    .or_default()
                    .insert(block_number as u32);

                for key in &tx_event.keys {
                    self.event_by_key
                        .entry(*key)
                        .or_default()
                        .insert(block_number as u32);
                }
            }
        }
        Ok(())
    }

    pub fn join(&mut self, other: &Self) {
        for (address, bitmap) in &other.event_by_address {
            let entry = self.event_by_address.entry(*address).or_default();
            *entry |= bitmap;
        }

        for (key, bitmap) in &other.event_by_key {
            let entry = self.event_by_key.entry(*key).or_default();
            *entry |= bitmap;
        }
    }
}

impl Debug for SegmentIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentIndex")
            .field("event_by_address", &self.event_by_address.len())
            .field("event_by_key", &self.event_by_key.len())
            .finish()
    }
}

impl<'fbb> FlatBufferBuilderFieldElementBitmapExt<'fbb> for FlatBufferBuilder<'fbb> {
    fn create_field_element_bitmap<'a: 'b, 'b>(
        &'a mut self,
        table: &'b BTreeMap<models::FieldElement, RoaringBitmap>,
    ) -> Result<
        WIPOffset<flatbuffers::Vector<'fbb, ForwardsUOffset<store::FieldElementBitmap<'fbb>>>>,
    > {
        let mut items = Vec::new();
        for (key, bitmap) in table {
            let mut buf = Vec::with_capacity(bitmap.serialized_size());
            bitmap
                .serialize_into(&mut buf)
                .change_context(DnaError::Fatal)
                .attach_printable("failed to serialize bitmap")?;
            let bitmap = self.create_vector(&buf);

            let key: store::FieldElement = key.into();
            let mut value = store::FieldElementBitmapBuilder::new(self);
            value.add_key(&key);
            value.add_bitmap(bitmap);

            let value = value.finish();
            items.push(value);
        }
        Ok(self.create_vector(&items))
    }
}
