use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

use super::{
    header::BlockHeaderBuilderExt, log::LogBuilderExt, receipt::TransactionReceiptBuilderExt,
    transaction::TransactionBuilderExt,
};

pub struct SingleBlockBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    header: Option<WIPOffset<store::BlockHeader<'a>>>,
    transactions: Vec<WIPOffset<store::Transaction<'a>>>,
    receipts: Vec<WIPOffset<store::TransactionReceipt<'a>>>,
    logs: Vec<WIPOffset<store::Log<'a>>>,
}

impl<'a> SingleBlockBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            header: None,
            transactions: Vec::new(),
            receipts: Vec::new(),
            logs: Vec::new(),
        }
    }

    pub fn add_block_header(&mut self, header: &models::Block) {
        self.header = Some(store::BlockHeaderBuilder::create_header(
            &mut self.builder,
            header,
        ));
    }

    pub fn add_transactions(&mut self, transactions: &[models::Transaction]) {
        self.transactions = transactions
            .iter()
            .map(|tx| store::TransactionBuilder::create_transaction(&mut self.builder, tx))
            .collect::<Vec<_>>();
    }

    pub fn add_receipts(&mut self, receipts: &[models::TransactionReceipt]) {
        self.receipts = receipts
            .iter()
            .map(|rx| store::TransactionReceiptBuilder::create_receipt(&mut self.builder, rx))
            .collect::<Vec<_>>();
    }

    pub fn add_logs(&mut self, receipts: &[models::TransactionReceipt]) {
        self.logs = receipts
            .iter()
            .flat_map(|receipt| receipt.logs.iter())
            .map(|log| store::LogBuilder::create_log(&mut self.builder, log))
            .collect::<Vec<_>>();
    }

    pub async fn write_block<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let header = self
            .header
            .ok_or(DnaError::Fatal)
            .attach_printable("writing single block without header")?;
        let transactions = self.builder.create_vector(&self.transactions);
        let receipts = self.builder.create_vector(&self.receipts);
        let logs = self.builder.create_vector(&self.logs);

        let mut single = store::SingleBlockBuilder::new(&mut self.builder);
        single.add_header(header);
        single.add_transactions(transactions);
        single.add_receipts(receipts);
        single.add_logs(logs);

        let single = single.finish();
        self.builder.finish(single, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write single block")?;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.builder.reset();
    }
}
