use apibara_dna_common::{
    error::{DnaError, Result},
    storage::FormattedSize,
};
use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::info;

use crate::provider::models;

use super::store;

pub struct EventSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    blocks: Vec<WIPOffset<store::BlockEvents<'a>>>,
}

impl<'a> EventSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            blocks: Vec::new(),
        }
    }

    pub fn add_block_events(
        &mut self,
        block_number: u64,
        receipts: &[models::TransactionReceipt],
    ) -> Result<()> {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        let mut all_events = Vec::new();

        for (tx_index, receipt) in receipts.iter().enumerate() {
            use models::TransactionReceipt::*;
            let tx_events = match receipt {
                Invoke(receipt) => receipt.events.iter(),
                L1Handler(receipt) => receipt.events.iter(),
                Declare(receipt) => receipt.events.iter(),
                Deploy(receipt) => receipt.events.iter(),
                DeployAccount(receipt) => receipt.events.iter(),
            };

            for tx_event in tx_events {
                let keys = {
                    let mut keys = Vec::new();
                    for key in tx_event.keys.iter() {
                        let key: store::FieldElement = key.into();
                        keys.push(key);
                    }
                    self.builder.create_vector(&keys)
                };

                let data = {
                    let mut data = Vec::new();
                    for datum in tx_event.data.iter() {
                        let datum: store::FieldElement = datum.into();
                        data.push(datum);
                    }
                    self.builder.create_vector(&data)
                };

                let mut event = store::EventBuilder::new(&mut self.builder);

                let from_address = tx_event.from_address.into();
                event.add_from_address(&from_address);
                event.add_keys(keys);
                event.add_data(data);
                let transaction_hash = receipt.transaction_hash().into();
                event.add_transaction_hash(&transaction_hash);
                event.add_transaction_index(tx_index as u64);

                all_events.push(event.finish());
            }
        }

        let events = self.builder.create_vector(&all_events);
        let block = {
            let mut block = store::BlockEventsBuilder::new(&mut self.builder);
            block.add_block_number(block_number);
            block.add_events(events);
            block.finish()
        };

        self.blocks.push(block);

        Ok(())
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let blocks = self.builder.create_vector(&self.blocks);

        let segment = {
            let first_block_number = self
                .first_block_number
                .ok_or(DnaError::Fatal)
                .attach_printable("first block number not set")?;
            let mut segment = store::EventSegmentBuilder::new(&mut self.builder);
            segment.add_first_block_number(first_block_number);
            segment.add_blocks(blocks);
            segment.finish()
        };

        self.builder.finish(segment, None);
        let bytes = self.builder.finished_data();
        info!(segment_size = %FormattedSize(bytes.len()), "flushing event segment");

        writer
            .write_all(bytes)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write event segment")?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        self.first_block_number = None;
        self.blocks.clear();
        self.builder.reset();

        Ok(())
    }
}
