use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    ingestion::models,
    segment::{conversion::U64Ext, store},
};

pub struct LogSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    blocks: Vec<WIPOffset<store::BlockLogs<'a>>>,
}

impl<'a> LogSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            blocks: Vec::new(),
        }
    }

    pub fn add_logs(&mut self, block_number: u64, receipts: &[models::TransactionReceipt]) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        let logs = receipts
            .iter()
            .flat_map(|receipt| receipt.logs.iter())
            .map(|log| self.add_single_log(log))
            .collect::<Vec<_>>();
        let logs = self.builder.create_vector(&logs);

        let mut block = store::BlockLogsBuilder::new(&mut self.builder);
        block.add_block_number(block_number);
        block.add_logs(logs);

        self.blocks.push(block.finish());
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let first_block_number = self
            .first_block_number
            .ok_or(DnaError::Fatal)
            .attach_printable("writing log segment but no block ingested")?;

        let blocks = self.builder.create_vector(&self.blocks);
        let mut segment = store::LogSegmentBuilder::new(&mut self.builder);
        segment.add_first_block_number(first_block_number);
        segment.add_blocks(blocks);

        let segment = segment.finish();
        self.builder.finish(segment, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write log segment")?;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.first_block_number = None;
        self.blocks.clear();
        self.builder.reset();
    }

    fn add_single_log(&mut self, log: &models::Log) -> WIPOffset<store::Log<'a>> {
        let topics = {
            let topics: Vec<store::B256> = log
                .topics
                .iter()
                .map(|uncle| uncle.into())
                .collect::<Vec<_>>();
            self.builder.create_vector(&topics)
        };
        let data = self.builder.create_vector(&log.data.0);

        let mut out = store::LogBuilder::new(&mut self.builder);

        out.add_address(&log.address.into());
        out.add_topics(topics);
        out.add_data(data);
        if let Some(log_index) = log.log_index {
            out.add_log_index(log_index.as_u64());
        }
        if let Some(transaction_index) = log.transaction_index {
            out.add_transaction_index(transaction_index.as_u64());
        }
        if let Some(transaction_hash) = log.transaction_hash {
            out.add_transaction_hash(&transaction_hash.into());
        }

        out.finish()
    }
}
