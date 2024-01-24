use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

use super::conversion::H64Ext;

pub struct BlockHeaderSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    headers: Vec<WIPOffset<store::BlockHeader<'a>>>,
    first_block_number: Option<u64>,
}

impl<'a> BlockHeaderSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            headers: vec![],
            first_block_number: None,
        }
    }

    pub fn add_block_header(&mut self, block_number: u64, block: &models::Block<models::H256>) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        let extra_data = self.builder.create_vector(&block.extra_data.0);
        let uncles = {
            let uncles: Vec<store::B256> = block
                .uncles
                .iter()
                .map(|uncle| uncle.into())
                .collect::<Vec<_>>();
            self.builder.create_vector(&uncles)
        };
        let withdrawals = if let Some(withdrawals) = &block.withdrawals {
            let withdrawals = withdrawals
                .iter()
                .map(|withdrawal| self.create_withdrawal(withdrawal))
                .collect::<Vec<_>>();
            Some(self.builder.create_vector(&withdrawals))
        } else {
            None
        };

        let mut header = store::BlockHeaderBuilder::new(&mut self.builder);

        header.add_number(block_number);
        if let Some(hash) = block.hash {
            header.add_hash(&hash.into());
        }
        header.add_parent_hash(&block.parent_hash.into());
        header.add_uncles_hash(&block.uncles_hash.into());
        if let Some(author) = block.author {
            header.add_miner(&author.into());
        }
        header.add_state_root(&block.state_root.into());
        header.add_transactions_root(&block.transactions_root.into());
        header.add_receipts_root(&block.receipts_root.into());
        if let Some(logs_bloom) = block.logs_bloom {
            header.add_logs_bloom(&logs_bloom.into());
        }
        header.add_difficulty(&block.difficulty.into());
        header.add_gas_limit(&block.gas_limit.into());
        header.add_gas_used(&block.gas_used.into());
        header.add_timestamp(&block.timestamp.into());
        header.add_extra_data(extra_data);
        if let Some(mix_hash) = block.mix_hash {
            header.add_mix_hash(&mix_hash.into());
        }
        if let Some(nonce) = block.nonce {
            header.add_nonce(nonce.into_u64());
        }
        if let Some(base_fee_per_gas) = block.base_fee_per_gas {
            header.add_base_fee_per_gas(&base_fee_per_gas.into());
        }
        if let Some(withdrawals_root) = block.withdrawals_root {
            header.add_withdrawals_root(&withdrawals_root.into());
        }
        if let Some(total_difficulty) = block.total_difficulty {
            header.add_total_difficulty(&total_difficulty.into());
        }
        header.add_uncles(uncles);
        if let Some(size) = block.size {
            header.add_size_(&size.into());
        }
        if let Some(withdrawals) = withdrawals {
            header.add_withdrawals(withdrawals);
        }

        self.headers.push(header.finish());
    }

    fn create_withdrawal(
        &mut self,
        withdrawal: &models::Withdrawal,
    ) -> WIPOffset<store::Withdrawal<'a>> {
        let mut out = store::WithdrawalBuilder::new(&mut self.builder);
        out.add_index(withdrawal.index.as_u64());
        out.add_validator_index(withdrawal.validator_index.as_u64());
        out.add_address(&withdrawal.address.into());
        out.add_amount(&withdrawal.amount.into());
        out.finish()
    }

    pub async fn write_segment<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<()> {
        let first_block_number = self
            .first_block_number
            .ok_or(DnaError::Fatal)
            .attach_printable("writing header segment but no block ingested")?;

        let headers = self.builder.create_vector(&self.headers);
        let mut segment = store::BlockHeaderSegmentBuilder::new(&mut self.builder);
        segment.add_first_block_number(first_block_number);
        segment.add_headers(headers);

        let segment = segment.finish();
        self.builder.finish(segment, None);

        writer
            .write_all(self.builder.finished_data())
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to write header segment")?;

        Ok(())
    }

    pub fn reset(&mut self) {
        self.first_block_number = None;
        self.headers.clear();
        self.builder.reset();
    }
}
