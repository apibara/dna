use error_stack::ResultExt;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use apibara_dna_common::error::{DnaError, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{ingestion::models, segment::store};

use crate::segment::conversion::model::U64Ext;

pub struct BlockHeaderSegmentBuilder<'a> {
    builder: FlatBufferBuilder<'a>,
    headers: Vec<WIPOffset<store::BlockHeader<'a>>>,
    first_block_number: Option<u64>,
    block_count: usize,
}

impl<'a> BlockHeaderSegmentBuilder<'a> {
    pub fn new() -> Self {
        Self {
            builder: FlatBufferBuilder::new(),
            headers: vec![],
            first_block_number: None,
            block_count: 0,
        }
    }

    pub fn block_count(&self) -> usize {
        self.block_count
    }

    pub fn copy_block_header<'b>(
        &mut self,
        block_number: u64,
        block_header: &store::BlockHeader<'b>,
    ) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        self.block_count += 1;

        self.headers.push(store::BlockHeaderBuilder::copy_header(
            &mut self.builder,
            block_header,
        ));
    }

    pub fn add_block_header(&mut self, block_number: u64, block: &models::Block) {
        if self.first_block_number.is_none() {
            self.first_block_number = Some(block_number);
        }

        self.block_count += 1;

        self.headers.push(store::BlockHeaderBuilder::create_header(
            &mut self.builder,
            block,
        ));
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
        self.block_count = 0;
        self.headers.clear();
        self.builder.reset();
    }
}

pub trait BlockHeaderBuilderExt<'a: 'b, 'b> {
    fn create_header(
        builder: &'b mut FlatBufferBuilder<'a>,
        header: &models::Block,
    ) -> WIPOffset<store::BlockHeader<'a>>;

    fn copy_header<'c>(
        builder: &'b mut FlatBufferBuilder<'a>,
        header: &store::BlockHeader<'c>,
    ) -> WIPOffset<store::BlockHeader<'a>>;
}

impl<'a: 'b, 'b> BlockHeaderBuilderExt<'a, 'b> for store::BlockHeaderBuilder<'a, 'b> {
    fn create_header(
        builder: &'b mut FlatBufferBuilder<'a>,
        block: &models::Block,
    ) -> WIPOffset<store::BlockHeader<'a>> {
        let header = &block.header;

        let extra_data = builder.create_vector(&header.extra_data.0);
        let uncles = {
            let uncles: Vec<store::B256> = block
                .uncles
                .iter()
                .map(|uncle| uncle.into())
                .collect::<Vec<_>>();
            builder.create_vector(&uncles)
        };
        let withdrawals = if let Some(withdrawals) = &block.withdrawals {
            let withdrawals = withdrawals
                .iter()
                .map(|withdrawal| {
                    let mut out = store::WithdrawalBuilder::new(builder);
                    out.add_index(withdrawal.index);
                    out.add_validator_index(withdrawal.validator_index);
                    out.add_address(&withdrawal.address.into());
                    out.add_amount(&models::U256::from(withdrawal.amount).into());
                    out.finish()
                })
                .collect::<Vec<_>>();
            Some(builder.create_vector(&withdrawals))
        } else {
            None
        };

        let mut out = store::BlockHeaderBuilder::new(builder);

        if let Some(number) = header.number {
            out.add_number(number.as_u64());
        }
        if let Some(hash) = header.hash {
            out.add_hash(&hash.into());
        }
        out.add_parent_hash(&header.parent_hash.into());
        out.add_uncles_hash(&header.uncles_hash.into());
        out.add_miner(&header.miner.into());
        out.add_state_root(&header.state_root.into());
        out.add_transactions_root(&header.transactions_root.into());
        out.add_receipts_root(&header.receipts_root.into());
        out.add_logs_bloom(&header.logs_bloom.into());
        out.add_difficulty(&header.difficulty.into());
        out.add_gas_limit(&header.gas_limit.into());
        out.add_gas_used(&header.gas_used.into());
        out.add_timestamp(&header.timestamp.into());
        out.add_extra_data(extra_data);
        if let Some(mix_hash) = header.mix_hash {
            out.add_mix_hash(&mix_hash.into());
        }
        if let Some(nonce) = header.nonce {
            out.add_nonce(nonce.as_u64());
        }
        if let Some(base_fee_per_gas) = header.base_fee_per_gas {
            out.add_base_fee_per_gas(&base_fee_per_gas.into());
        }
        if let Some(withdrawals_root) = header.withdrawals_root {
            out.add_withdrawals_root(&withdrawals_root.into());
        }
        if let Some(total_difficulty) = block.total_difficulty {
            out.add_total_difficulty(&total_difficulty.into());
        }
        out.add_uncles(uncles);
        if let Some(size) = block.size {
            out.add_size_(&size.into());
        }
        if let Some(withdrawals) = withdrawals {
            out.add_withdrawals(withdrawals);
        }
        if let Some(blob_gas_used) = header.blob_gas_used {
            out.add_blob_gas_used(blob_gas_used.as_u64());
        }
        if let Some(excess_blob_gas) = header.excess_blob_gas {
            out.add_excess_blob_gas(excess_blob_gas.as_u64());
        }
        if let Some(parent_beacon_block_root) = header.parent_beacon_block_root {
            out.add_parent_beacon_block_root(&parent_beacon_block_root.into());
        }

        out.finish()
    }

    fn copy_header<'c>(
        builder: &'b mut FlatBufferBuilder<'a>,
        header: &store::BlockHeader<'c>,
    ) -> WIPOffset<store::BlockHeader<'a>> {
        let extra_data =
            builder.create_vector_from_iter(header.extra_data().unwrap_or_default().iter());

        let uncles = builder.create_vector_from_iter(header.uncles().unwrap_or_default().iter());
        let withdrawals = {
            let items = header
                .withdrawals()
                .unwrap_or_default()
                .iter()
                .map(|w| {
                    let mut out = store::WithdrawalBuilder::new(builder);
                    out.add_index(w.index());
                    out.add_validator_index(w.validator_index());
                    if let Some(address) = w.address() {
                        out.add_address(address);
                    }
                    if let Some(amount) = w.amount() {
                        out.add_amount(amount);
                    }
                    out.finish()
                })
                .collect::<Vec<_>>();
            builder.create_vector(&items)
        };

        let mut out = store::BlockHeaderBuilder::new(builder);

        out.add_number(header.number());
        if let Some(hash) = header.hash() {
            out.add_hash(hash);
        }
        if let Some(parent_hash) = header.parent_hash() {
            out.add_parent_hash(parent_hash);
        }
        if let Some(uncles_hash) = header.uncles_hash() {
            out.add_uncles_hash(uncles_hash);
        }
        if let Some(miner) = header.miner() {
            out.add_miner(miner);
        }
        if let Some(state_root) = header.state_root() {
            out.add_state_root(state_root);
        }
        if let Some(transactions_root) = header.transactions_root() {
            out.add_transactions_root(transactions_root);
        }
        if let Some(receipts_root) = header.receipts_root() {
            out.add_receipts_root(receipts_root);
        }
        if let Some(logs_bloom) = header.logs_bloom() {
            out.add_logs_bloom(logs_bloom);
        }
        if let Some(difficulty) = header.difficulty() {
            out.add_difficulty(difficulty);
        }
        if let Some(gas_limit) = header.gas_limit() {
            out.add_gas_limit(gas_limit);
        }
        if let Some(gas_used) = header.gas_used() {
            out.add_gas_used(gas_used);
        }
        if let Some(timestamp) = header.timestamp() {
            out.add_timestamp(timestamp);
        }
        out.add_extra_data(extra_data);
        if let Some(mix_hash) = header.mix_hash() {
            out.add_mix_hash(mix_hash);
        }
        out.add_nonce(header.nonce());
        if let Some(base_fee_per_gas) = header.base_fee_per_gas() {
            out.add_base_fee_per_gas(base_fee_per_gas);
        }
        if let Some(withdrawals_root) = header.withdrawals_root() {
            out.add_withdrawals_root(withdrawals_root);
        }
        if let Some(total_difficulty) = header.total_difficulty() {
            out.add_total_difficulty(total_difficulty);
        }
        out.add_uncles(uncles);
        if let Some(size) = header.size_() {
            out.add_size_(size);
        }
        out.add_withdrawals(withdrawals);
        out.add_blob_gas_used(header.blob_gas_used());
        out.add_excess_blob_gas(header.excess_blob_gas());
        if let Some(parent_beacon_block_root) = header.parent_beacon_block_root() {
            out.add_parent_beacon_block_root(parent_beacon_block_root);
        }

        out.finish()
    }
}
