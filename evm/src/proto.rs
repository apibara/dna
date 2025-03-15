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
            log_index_in_transaction: u32::MAX,
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

impl ModelExt for models::TraceResults {
    type Proto = evm::TransactionTrace;

    fn to_proto(&self) -> Self::Proto {
        evm::TransactionTrace {
            filter_ids: Vec::default(),
            transaction_index: u32::MAX,
            transaction_hash: None,
            traces: self.trace.iter().map(ModelExt::to_proto).collect(),
        }
    }
}

impl ModelExt for models::TransactionTrace {
    type Proto = evm::Trace;

    fn to_proto(&self) -> Self::Proto {
        evm::Trace {
            action: self.action.to_proto().into(),
            error: self.error.clone(),
            output: self.result.as_ref().map(ModelExt::to_proto),
            subtraces: self.subtraces as u32,
            trace_address: self.trace_address.iter().map(|a| *a as u32).collect(),
        }
    }
}

impl ModelExt for models::Action {
    type Proto = evm::trace::Action;

    fn to_proto(&self) -> Self::Proto {
        use models::Action::*;

        match self {
            Create(action) => evm::trace::Action::Create(action.to_proto()),
            Call(action) => evm::trace::Action::Call(action.to_proto()),
            Selfdestruct(action) => evm::trace::Action::SelfDestruct(action.to_proto()),
            Reward(action) => evm::trace::Action::Reward(action.to_proto()),
        }
    }
}

impl ModelExt for models::CallAction {
    type Proto = evm::CallAction;

    fn to_proto(&self) -> Self::Proto {
        evm::CallAction {
            from_address: self.from.to_proto().into(),
            r#type: self.call_type.to_proto(),
            gas: self.gas,
            input: self.input.to_vec(),
            to_address: self.to.to_proto().into(),
            value: self.value.to_proto().into(),
        }
    }
}

impl ModelExt for models::CreateAction {
    type Proto = evm::CreateAction;

    fn to_proto(&self) -> Self::Proto {
        evm::CreateAction {
            from_address: self.from.to_proto().into(),
            gas: self.gas,
            init: self.init.to_vec(),
            value: self.value.to_proto().into(),
            creation_method: 0,
        }
    }
}

impl ModelExt for models::SelfdestructAction {
    type Proto = evm::SelfDestructAction;

    fn to_proto(&self) -> Self::Proto {
        evm::SelfDestructAction {
            address: self.address.to_proto().into(),
            balance: self.balance.to_proto().into(),
            refund_address: self.refund_address.to_proto().into(),
        }
    }
}

impl ModelExt for models::RewardAction {
    type Proto = evm::RewardAction;

    fn to_proto(&self) -> Self::Proto {
        evm::RewardAction {
            author: self.author.to_proto().into(),
            r#type: self.reward_type.to_proto(),
            value: self.value.to_proto().into(),
        }
    }
}

impl ModelExt for models::TraceOutput {
    type Proto = evm::trace::Output;

    fn to_proto(&self) -> Self::Proto {
        use models::TraceOutput::*;

        match self {
            Call(output) => evm::trace::Output::CallOutput(output.to_proto()),
            Create(output) => evm::trace::Output::CreateOutput(output.to_proto()),
        }
    }
}

impl ModelExt for models::CallOutput {
    type Proto = evm::CallOutput;

    fn to_proto(&self) -> Self::Proto {
        evm::CallOutput {
            gas_used: self.gas_used,
            output: self.output.to_vec(),
        }
    }
}

impl ModelExt for models::CreateOutput {
    type Proto = evm::CreateOutput;

    fn to_proto(&self) -> Self::Proto {
        evm::CreateOutput {
            address: self.address.to_proto().into(),
            code: self.code.to_vec(),
            gas_used: self.gas_used,
        }
    }
}

impl ModelExt for models::CallType {
    type Proto = i32;

    fn to_proto(&self) -> Self::Proto {
        use models::CallType::*;

        match self {
            None => evm::CallType::Unspecified as i32,
            Call => evm::CallType::Call as i32,
            CallCode => evm::CallType::CallCode as i32,
            DelegateCall => evm::CallType::DelegateCall as i32,
            StaticCall => evm::CallType::StaticCall as i32,
            AuthCall => evm::CallType::AuthCall as i32,
        }
    }
}

impl ModelExt for models::RewardType {
    type Proto = i32;

    fn to_proto(&self) -> Self::Proto {
        use models::RewardType::*;

        match self {
            Block => evm::RewardType::Block as i32,
            Uncle => evm::RewardType::Uncle as i32,
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
