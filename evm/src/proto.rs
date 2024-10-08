use apibara_dna_protocol::evm;

use crate::provider::models;

pub trait ModelExt {
    type Proto;
    fn to_proto(&self) -> Self::Proto;
}

pub fn convert_block_header(block: models::Header) -> evm::BlockHeader {
    let timestamp = prost_types::Timestamp {
        seconds: block.timestamp as i64,
        nanos: 0,
    };

    evm::BlockHeader {
        block_number: block.number,
        block_hash: block.hash.to_proto().into(),
        parent_block_hash: block.parent_hash.to_proto().into(),
        uncles_hash: block.uncles_hash.to_proto().into(),
        miner: block.miner.to_proto().into(),
        state_root: block.state_root.to_proto().into(),
        transactions_root: block.transactions_root.to_proto().into(),
        receipts_root: block.receipts_root.to_proto().into(),
        logs_bloom: block.logs_bloom.to_proto().into(),
        difficulty: block.difficulty.to_proto().into(),
        gas_limit: block.gas_limit.to_proto().into(),
        gas_used: block.gas_used.to_proto().into(),
        timestamp: timestamp.into(),
        extra_data: block.extra_data.to_vec(),
        mix_hash: block.mix_hash.as_ref().map(ModelExt::to_proto),
        nonce: block.nonce.map(|n| u64::from_be_bytes(n.0)),
        base_fee_per_gas: block.base_fee_per_gas.as_ref().map(ModelExt::to_proto),
        withdrawals_root: block.withdrawals_root.as_ref().map(ModelExt::to_proto),
        total_difficulty: block.total_difficulty.as_ref().map(ModelExt::to_proto),
        blob_gas_used: block.blob_gas_used.as_ref().map(ModelExt::to_proto),
        excess_blob_gas: block.excess_blob_gas.as_ref().map(ModelExt::to_proto),
        parent_beacon_block_root: block
            .parent_beacon_block_root
            .as_ref()
            .map(ModelExt::to_proto),
    }
}

impl ModelExt for models::Transaction {
    type Proto = evm::Transaction;

    fn to_proto(&self) -> Self::Proto {
        evm::Transaction {
            filter_ids: Vec::new(),
            transaction_index: u32::MAX,
            transaction_hash: None,
            nonce: self.nonce,
            from: self.from.to_proto().into(),
            to: self.to.as_ref().map(ModelExt::to_proto),
            value: self.value.to_proto().into(),
            gas_price: self.gas_price.as_ref().map(ModelExt::to_proto),
            gas: self.gas.to_proto().into(),
            max_fee_per_gas: self.max_fee_per_gas.as_ref().map(ModelExt::to_proto),
            max_priority_fee_per_gas: self
                .max_priority_fee_per_gas
                .as_ref()
                .map(ModelExt::to_proto),
            input: self.input.to_vec(),
            signature: self.signature.as_ref().map(ModelExt::to_proto),
            chain_id: self.chain_id,
            access_list: self
                .access_list
                .as_ref()
                .map(|l| l.iter().map(ModelExt::to_proto).collect())
                .unwrap_or_default(),
            transaction_type: self.transaction_type.unwrap_or_default() as u64,
            max_fee_per_blob_gas: self.max_fee_per_blob_gas.as_ref().map(ModelExt::to_proto),
            blob_versioned_hashes: self
                .blob_versioned_hashes
                .as_ref()
                .map(|l| l.iter().map(ModelExt::to_proto).collect())
                .unwrap_or_default(),
            transaction_status: 0,
        }
    }
}

impl ModelExt for models::Withdrawal {
    type Proto = evm::Withdrawal;

    fn to_proto(&self) -> Self::Proto {
        evm::Withdrawal {
            filter_ids: Vec::new(),
            withdrawal_index: u32::MAX,
            index: self.index,
            validator_index: self.validator_index as u32,
            address: self.address.to_proto().into(),
            amount: self.amount,
        }
    }
}

impl ModelExt for models::TransactionReceipt {
    type Proto = evm::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        evm::TransactionReceipt {
            filter_ids: Vec::new(),
            transaction_index: u32::MAX,
            transaction_hash: self.transaction_hash.to_proto().into(),
            cumulative_gas_used: self.inner.cumulative_gas_used().to_proto().into(),
            gas_used: self.gas_used.to_proto().into(),
            effective_gas_price: self.effective_gas_price.to_proto().into(),
            from: self.from.to_proto().into(),
            to: self.to.as_ref().map(ModelExt::to_proto),
            contract_address: self.contract_address.as_ref().map(ModelExt::to_proto),
            logs_bloom: self.inner.logs_bloom().to_proto().into(),
            transaction_type: self.transaction_type() as u8 as u64,
            blob_gas_used: self.blob_gas_used.as_ref().map(ModelExt::to_proto),
            blob_gas_price: self.blob_gas_price.as_ref().map(ModelExt::to_proto),
            transaction_status: if self.status() {
                evm::TransactionStatus::Succeeded as i32
            } else {
                evm::TransactionStatus::Reverted as i32
            },
        }
    }
}

impl ModelExt for models::Log {
    type Proto = evm::Log;

    fn to_proto(&self) -> Self::Proto {
        evm::Log {
            filter_ids: Vec::new(),
            log_index: u32::MAX,
            address: self.address().to_proto().into(),
            topics: self.topics().iter().map(ModelExt::to_proto).collect(),
            data: self.inner.data.data.to_vec(),
            transaction_index: u32::MAX,
            transaction_hash: None,
            transaction_status: 0,
        }
    }
}

impl ModelExt for models::Signature {
    type Proto = evm::Signature;

    fn to_proto(&self) -> Self::Proto {
        evm::Signature {
            r: self.r.to_proto().into(),
            s: self.s.to_proto().into(),
            v: self.v.to_proto().into(),
            y_parity: self.y_parity.map(|p| p.0),
        }
    }
}

impl ModelExt for models::AccessListItem {
    type Proto = evm::AccessListItem;

    fn to_proto(&self) -> Self::Proto {
        evm::AccessListItem {
            address: self.address.to_proto().into(),
            storage_keys: self.storage_keys.iter().map(ModelExt::to_proto).collect(),
        }
    }
}

impl ModelExt for models::B256 {
    type Proto = evm::B256;

    fn to_proto(&self) -> Self::Proto {
        evm::B256::from_bytes(&self.0)
    }
}

impl ModelExt for models::U256 {
    type Proto = evm::U256;

    fn to_proto(&self) -> Self::Proto {
        evm::U256::from_bytes(&self.to_be_bytes())
    }
}

impl ModelExt for models::Address {
    type Proto = evm::Address;

    fn to_proto(&self) -> Self::Proto {
        evm::Address::from_bytes(&self.0)
    }
}

impl ModelExt for models::Bloom {
    type Proto = evm::Bloom;

    fn to_proto(&self) -> Self::Proto {
        evm::Bloom {
            value: self.0.to_vec(),
        }
    }
}

impl ModelExt for u128 {
    type Proto = evm::U128;

    fn to_proto(&self) -> Self::Proto {
        evm::U128::from_bytes(&self.to_be_bytes())
    }
}
