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

pub struct BlockHeaderSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    first_block_number: Option<u64>,
    headers: Vec<WIPOffset<store::BlockHeader<'a>>>,
}

impl<'a> BlockHeaderSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            first_block_number: None,
            headers: Vec::new(),
        }
    }

    pub fn add_block_header(&mut self, block: &models::BlockWithTxHashes) -> Result<()> {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block.block_number);
        }

        let header = {
            let mut l1_gas_price = store::ResourcePriceBuilder::new(&mut self.builder);
            let price_in_fri = block.l1_gas_price.price_in_fri.into();
            l1_gas_price.add_price_in_fri(&price_in_fri);
            let price_in_wei = block.l1_gas_price.price_in_wei.into();
            l1_gas_price.add_price_in_wei(&price_in_wei);
            let l1_gas_price = l1_gas_price.finish();

            let starknet_version = self.builder.create_string(&block.starknet_version);

            let mut header = store::BlockHeaderBuilder::new(&mut self.builder);

            let block_hash = block.block_hash.into();
            header.add_block_hash(&block_hash);
            let parent_hash = block.parent_hash.into();
            header.add_parent_hash(&parent_hash);
            header.add_block_number(block.block_number);
            let new_root = block.new_root.into();
            header.add_new_root(&new_root);
            header.add_timestamp(block.timestamp);
            let sequencer_address = block.sequencer_address.into();
            header.add_sequencer_address(&sequencer_address);
            header.add_l1_gas_price(l1_gas_price);
            header.add_starknet_version(starknet_version);

            header.finish()
        };

        self.headers.push(header);

        Ok(())
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let headers = self.builder.create_vector(&self.headers);

        let segment = {
            let first_block_number = self
                .first_block_number
                .ok_or(DnaError::Fatal)
                .attach_printable("first block number not set")?;
            let mut segment = store::BlockHeaderSegmentBuilder::new(&mut self.builder);
            segment.add_first_block_number(first_block_number);
            segment.add_headers(headers);
            segment.finish()
        };

        self.builder.finish(segment, None);

        let bytes = self.builder.finished_data();
        info!(segment_size = %FormattedSize(bytes.len()), "flushing block header segment");
        writer
            .write_all(bytes)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write block header segment")?;
        writer.shutdown().await.change_context(DnaError::Io)?;

        self.first_block_number = None;
        self.headers.clear();
        self.builder.reset();

        Ok(())
    }
}
