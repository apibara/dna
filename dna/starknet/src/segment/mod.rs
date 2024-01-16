use std::fmt::Display;

use apibara_dna_common::error::Result;
use byte_unit::Byte;
use tracing::info;

mod common;
mod event;
mod header;
#[allow(dead_code, unused_imports)]
mod store;

use crate::provider::models;

pub use self::event::EventSegmentBuilder;
pub use self::header::BlockHeaderSegmentBuilder;

pub struct BlockSegmentBuilder<'a> {
    header: BlockHeaderSegmentBuilder<'a>,
    event: EventSegmentBuilder<'a>,
}

impl<'a> BlockSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            header: BlockHeaderSegmentBuilder::new(),
            event: EventSegmentBuilder::new(),
        }
    }

    pub fn add_block_header(&mut self, block: &models::BlockWithTxHashes) -> Result<()> {
        self.header.add_block_header(block)
    }

    pub fn add_events(
        &mut self,
        block_number: u64,
        receipts: &[models::TransactionReceipt],
    ) -> Result<()> {
        self.event.add_block_events(block_number, receipts)
    }

    pub fn add_transactions(&mut self, _transactions: &[models::Transaction]) -> Result<()> {
        Ok(())
    }

    pub fn add_receipts(&mut self, _receipts: &[models::TransactionReceipt]) -> Result<()> {
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        let header = self.header.finished_data()?;
        let event = self.event.finished_data()?;

        info!(
            header_size = %FormattedSize(header.len()),
            event_size = %FormattedSize(event.len()),
            "flushing block segment"
        );

        Ok(())
    }
}

pub struct FormattedSize(pub usize);

impl Display for FormattedSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let byte = Byte::from_bytes(self.0 as u128).get_appropriate_unit(true);
        byte.fmt(f)
    }
}
