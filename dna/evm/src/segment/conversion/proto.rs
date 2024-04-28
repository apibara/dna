use apibara_dna_protocol::evm;

use crate::segment::store;

impl From<&evm::Address> for store::Address {
    fn from(value: &evm::Address) -> Self {
        store::Address::new(&value.to_bytes())
    }
}

impl From<&evm::B256> for store::B256 {
    fn from(value: &evm::B256) -> Self {
        store::B256::new(&value.to_bytes())
    }
}

impl<'a> From<store::BlockHeader<'a>> for evm::BlockHeader {
    fn from(b: store::BlockHeader<'a>) -> Self {
        let timestamp = pbjson_types::Timestamp {
            seconds: b.timestamp() as i64,
            nanos: 0,
        };

        evm::BlockHeader {
            number: b.number(),
            hash: b.hash().map(Into::into),
            parent_hash: b.parent_hash().map(Into::into),
            uncles_hash: b.uncles_hash().map(Into::into),
            miner: b.miner().map(Into::into),
            state_root: b.state_root().map(Into::into),
            transactions_root: b.transactions_root().map(Into::into),
            receipts_root: b.receipts_root().map(Into::into),
            logs_bloom: b.logs_bloom().map(Into::into),
            difficulty: b.difficulty().map(Into::into),
            gas_limit: b.gas_limit().map(Into::into),
            gas_used: b.gas_used().map(Into::into),
            timestamp: timestamp.into(),
            extra_data: b.extra_data().map(|v| v.to_hex_data()),
            mix_hash: b.mix_hash().map(Into::into),
            nonce: b.nonce(),
            base_fee_per_gas: b.base_fee_per_gas().map(Into::into),
            withdrawals_root: b.withdrawals_root().map(Into::into),
            total_difficulty: b.total_difficulty().map(Into::into),
            uncles: b
                .uncles()
                .unwrap_or_default()
                .iter()
                .map(Into::into)
                .collect(),
            size: b.size_().map(Into::into),
            blob_gas_used: b.blob_gas_used(),
            excess_blob_gas: b.excess_blob_gas(),
            parent_beacon_block_root: b.parent_beacon_block_root().map(Into::into),
        }
    }
}

impl From<&store::B256> for evm::B256 {
    fn from(v: &store::B256) -> Self {
        evm::B256::from_bytes(&v.0)
    }
}

impl From<&store::U256> for evm::U256 {
    fn from(v: &store::U256) -> Self {
        evm::U256::from_bytes(&v.0)
    }
}

impl From<&store::U128> for evm::U128 {
    fn from(v: &store::U128) -> Self {
        evm::U128::from_bytes(&v.0)
    }
}

impl From<&store::Address> for evm::Address {
    fn from(v: &store::Address) -> Self {
        evm::Address::from_bytes(&v.0)
    }
}

impl From<&store::Bloom> for evm::Bloom {
    fn from(v: &store::Bloom) -> Self {
        evm::Bloom {
            value: v.0.to_vec(),
        }
    }
}

impl<'a> From<store::Withdrawal<'a>> for evm::Withdrawal {
    fn from(v: store::Withdrawal) -> Self {
        evm::Withdrawal {
            withdrawal_index: u64::MAX,
            address: v.address().map(Into::into),
            amount: v.amount().map(Into::into),
            index: v.index(),
            validator_index: v.validator_index(),
        }
    }
}

impl<'a> From<store::Transaction<'a>> for evm::Transaction {
    fn from(v: store::Transaction<'a>) -> Self {
        evm::Transaction {
            transaction_index: v.transaction_index(),
            hash: v.hash().map(Into::into),
            nonce: v.nonce(),
            from: v.from().map(Into::into),
            to: v.to().map(Into::into),
            value: v.value().map(Into::into),
            gas_price: v.gas_price().map(Into::into),
            gas: v.gas().map(Into::into),
            max_fee_per_gas: v.max_fee_per_gas().map(Into::into),
            max_priority_fee_per_gas: v.max_priority_fee_per_gas().map(Into::into),
            input: v.input().map(|v| v.to_hex_data()),
            signature: v.signature().map(Into::into),
            chain_id: v.chain_id(),
            access_list: v
                .access_list()
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            transaction_type: v.transaction_type(),
            max_fee_per_blob_gas: v.max_fee_per_blob_gas().map(Into::into),
            blob_versioned_hashes: v
                .blob_versioned_hashes()
                .unwrap_or_default()
                .iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl<'a> From<store::Signature<'a>> for evm::Signature {
    fn from(v: store::Signature<'a>) -> Self {
        evm::Signature {
            r: v.r().map(Into::into),
            s: v.s().map(Into::into),
            v: v.v().map(Into::into),
            y_parity: v.y_parity(),
        }
    }
}

impl<'a> From<store::AccessListItem<'a>> for evm::AccessListItem {
    fn from(v: store::AccessListItem<'a>) -> Self {
        evm::AccessListItem {
            address: v.address().map(Into::into),
            storage_keys: v
                .storage_keys()
                .unwrap_or_default()
                .iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl<'a> From<store::Log<'a>> for evm::Log {
    fn from(v: store::Log<'a>) -> Self {
        evm::Log {
            address: v.address().map(Into::into),
            topics: v
                .topics()
                .unwrap_or_default()
                .iter()
                .map(Into::into)
                .collect(),
            data: v.data().map(|v| v.to_hex_data()),
            log_index: v.log_index(),
            transaction_index: v.transaction_index(),
            transaction_hash: v.transaction_hash().map(Into::into),
        }
    }
}

impl<'a> From<store::TransactionReceipt<'a>> for evm::TransactionReceipt {
    fn from(value: store::TransactionReceipt<'a>) -> Self {
        evm::TransactionReceipt {
            transaction_hash: value.transaction_hash().map(Into::into),
            transaction_index: value.transaction_index(),
            cumulative_gas_used: value.cumulative_gas_used().map(Into::into),
            gas_used: value.gas_used().map(Into::into),
            effective_gas_price: value.effective_gas_price().map(Into::into),
            from: value.from().map(Into::into),
            to: value.to().map(Into::into),
            contract_address: value.contract_address().map(Into::into),
            logs_bloom: value.logs_bloom().map(Into::into),
            status_code: value.status_code(),
            transaction_type: value.transaction_type(),
            blob_gas_used: value.blob_gas_used().map(Into::into),
            blob_gas_price: value.blob_gas_price().map(Into::into),
        }
    }
}

trait BytesVectorExt {
    fn to_hex_data(&self) -> evm::HexData;
}

impl BytesVectorExt for flatbuffers::Vector<'_, u8> {
    fn to_hex_data(&self) -> evm::HexData {
        let value = self.iter().collect();
        evm::HexData { value }
    }
}
