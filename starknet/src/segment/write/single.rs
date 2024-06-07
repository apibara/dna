use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    ingestion::models,
    segment::{store, write::BlockHeaderBuilderExt},
};

pub struct SingleBlockBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    header: Option<WIPOffset<store::BlockHeader<'a>>>,
    events: Vec<WIPOffset<store::Event<'a>>>,
}

impl<'a> SingleBlockBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            header: None,
            events: Vec::new(),
        }
    }

    pub fn add_block_with_receipts(&mut self, block: &models::BlockWithReceipts) {
        self.header = Some(store::BlockHeaderBuilder::create_header(
            &mut self.builder,
            block,
        ));
    }

    pub async fn write_block<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<usize> {
        let header = self
            .header
            .ok_or(DnaError::Fatal)
            .attach_printable("writing single block without header")?;

        let mut single = store::SingleBlockBuilder::new(&mut self.builder);
        single.add_header(header);
        /*
        single.add_header(header);
        single.add_transactions(transactions);
        single.add_receipts(receipts);
        single.add_logs(logs);
        */

        let single = single.finish();
        self.builder.finish(single, None);

        let data_size = self.builder.finished_data().len();
        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write single block")?;

        Ok(data_size)
    }

    pub fn reset(&mut self) {
        self.builder.reset();
    }
}
