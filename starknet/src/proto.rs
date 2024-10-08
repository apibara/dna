use apibara_dna_protocol::starknet;

use crate::provider::models;

pub trait ModelExt {
    type Proto;
    fn to_proto(&self) -> Self::Proto;
}

pub fn convert_block_header(block: &models::BlockWithReceipts) -> starknet::BlockHeader {
    let timestamp = prost_types::Timestamp {
        seconds: block.timestamp as i64,
        nanos: 0,
    };

    starknet::BlockHeader {
        block_hash: block.block_hash.to_proto().into(),
        parent_block_hash: block.parent_hash.to_proto().into(),
        block_number: block.block_number,
        new_root: block.new_root.to_proto().into(),
        sequencer_address: block.sequencer_address.to_proto().into(),
        starknet_version: block.starknet_version.clone(),
        timestamp: timestamp.into(),
        l1_data_gas_price: block.l1_gas_price.to_proto().into(),
        l1_gas_price: block.l1_gas_price.to_proto().into(),
        l1_data_availability_mode: block.l1_da_mode.to_proto(),
    }
}

impl ModelExt for models::FieldElement {
    type Proto = starknet::FieldElement;

    fn to_proto(&self) -> starknet::FieldElement {
        starknet::FieldElement::from_bytes(&self.to_bytes_be())
    }
}

impl ModelExt for models::ResourcePrice {
    type Proto = starknet::ResourcePrice;

    fn to_proto(&self) -> Self::Proto {
        Self::Proto {
            price_in_fri: self.price_in_fri.to_proto().into(),
            price_in_wei: self.price_in_wei.to_proto().into(),
        }
    }
}

impl ModelExt for models::L1DataAvailabilityMode {
    type Proto = i32;

    fn to_proto(&self) -> Self::Proto {
        use models::L1DataAvailabilityMode::*;
        match self {
            Blob => starknet::L1DataAvailabilityMode::Blob as i32,
            Calldata => starknet::L1DataAvailabilityMode::Calldata as i32,
        }
    }
}

impl ModelExt for models::Transaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        use models::Transaction::*;

        match self {
            Invoke(tx) => tx.to_proto(),
            L1Handler(tx) => tx.to_proto(),
            Declare(tx) => tx.to_proto(),
            Deploy(tx) => tx.to_proto(),
            DeployAccount(tx) => tx.to_proto(),
        }
    }
}

impl ModelExt for models::InvokeTransaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        use models::InvokeTransaction::*;

        match self {
            V0(tx) => tx.to_proto(),
            V1(tx) => tx.to_proto(),
            V3(tx) => tx.to_proto(),
        }
    }
}

impl ModelExt for models::InvokeTransactionV0 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::InvokeTransactionV0 {
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            contract_address: self.contract_address.to_proto().into(),
            entry_point_selector: self.entry_point_selector.to_proto().into(),
            calldata: self.calldata.iter().map(ModelExt::to_proto).collect(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::InvokeV0(inner)),
        }
    }
}

impl ModelExt for models::InvokeTransactionV1 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::InvokeTransactionV1 {
            sender_address: self.sender_address.to_proto().into(),
            calldata: self.calldata.iter().map(ModelExt::to_proto).collect(),
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::InvokeV1(inner)),
        }
    }
}

impl ModelExt for models::InvokeTransactionV3 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::InvokeTransactionV3 {
            sender_address: self.sender_address.to_proto().into(),
            calldata: self.calldata.iter().map(ModelExt::to_proto).collect(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            resource_bounds: self.resource_bounds.to_proto().into(),
            tip: self.tip,
            paymaster_data: self.paymaster_data.iter().map(ModelExt::to_proto).collect(),
            account_deployment_data: self
                .account_deployment_data
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            nonce_data_availability_mode: self.nonce_data_availability_mode.to_proto(),
            fee_data_availability_mode: self.fee_data_availability_mode.to_proto(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::InvokeV3(inner)),
        }
    }
}

impl ModelExt for models::L1HandlerTransaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::L1HandlerTransaction {
            nonce: self.nonce,
            contract_address: self.contract_address.to_proto().into(),
            entry_point_selector: self.entry_point_selector.to_proto().into(),
            calldata: self.calldata.iter().map(ModelExt::to_proto).collect(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::L1Handler(inner)),
        }
    }
}

impl ModelExt for models::DeclareTransaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        use models::DeclareTransaction::*;

        match self {
            V0(tx) => tx.to_proto(),
            V1(tx) => tx.to_proto(),
            V2(tx) => tx.to_proto(),
            V3(tx) => tx.to_proto(),
        }
    }
}

impl ModelExt for models::DeclareTransactionV0 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeclareTransactionV0 {
            sender_address: self.sender_address.to_proto().into(),
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            class_hash: self.class_hash.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeclareV0(inner)),
        }
    }
}

impl ModelExt for models::DeclareTransactionV1 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeclareTransactionV1 {
            sender_address: self.sender_address.to_proto().into(),
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            class_hash: self.class_hash.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeclareV1(inner)),
        }
    }
}

impl ModelExt for models::DeclareTransactionV2 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeclareTransactionV2 {
            sender_address: self.sender_address.to_proto().into(),
            compiled_class_hash: self.compiled_class_hash.to_proto().into(),
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            class_hash: self.class_hash.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeclareV2(inner)),
        }
    }
}

impl ModelExt for models::DeclareTransactionV3 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeclareTransactionV3 {
            sender_address: self.sender_address.to_proto().into(),
            compiled_class_hash: self.compiled_class_hash.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            class_hash: self.class_hash.to_proto().into(),
            resource_bounds: self.resource_bounds.to_proto().into(),
            tip: self.tip,
            paymaster_data: self.paymaster_data.iter().map(ModelExt::to_proto).collect(),
            account_deployment_data: self
                .account_deployment_data
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            nonce_data_availability_mode: self.nonce_data_availability_mode.to_proto(),
            fee_data_availability_mode: self.fee_data_availability_mode.to_proto(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeclareV3(inner)),
        }
    }
}

impl ModelExt for models::DeployTransaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeployTransaction {
            contract_address_salt: self.contract_address_salt.to_proto().into(),
            constructor_calldata: self
                .constructor_calldata
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            class_hash: self.class_hash.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::Deploy(inner)),
        }
    }
}

impl ModelExt for models::DeployAccountTransaction {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        use models::DeployAccountTransaction::*;

        match self {
            V1(tx) => tx.to_proto(),
            V3(tx) => tx.to_proto(),
        }
    }
}

impl ModelExt for models::DeployAccountTransactionV1 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeployAccountTransactionV1 {
            max_fee: self.max_fee.to_proto().into(),
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            contract_address_salt: self.contract_address_salt.to_proto().into(),
            constructor_calldata: self
                .constructor_calldata
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            class_hash: self.class_hash.to_proto().into(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeployAccountV1(inner)),
        }
    }
}

impl ModelExt for models::DeployAccountTransactionV3 {
    type Proto = starknet::Transaction;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            transaction_index: u32::MAX,
            transaction_status: 0,
        };

        let inner = starknet::DeployAccountTransactionV3 {
            signature: self.signature.iter().map(ModelExt::to_proto).collect(),
            nonce: self.nonce.to_proto().into(),
            contract_address_salt: self.contract_address_salt.to_proto().into(),
            constructor_calldata: self
                .constructor_calldata
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            class_hash: self.class_hash.to_proto().into(),
            resource_bounds: self.resource_bounds.to_proto().into(),
            tip: self.tip,
            paymaster_data: self.paymaster_data.iter().map(ModelExt::to_proto).collect(),
            nonce_data_availability_mode: self.nonce_data_availability_mode.to_proto(),
            fee_data_availability_mode: self.fee_data_availability_mode.to_proto(),
        };

        starknet::Transaction {
            filter_ids: Vec::default(),
            meta: meta.into(),
            transaction: Some(starknet::transaction::Transaction::DeployAccountV3(inner)),
        }
    }
}

impl ModelExt for models::TransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        use models::TransactionReceipt::*;

        match self {
            Invoke(tx) => tx.to_proto(),
            L1Handler(tx) => tx.to_proto(),
            Declare(tx) => tx.to_proto(),
            Deploy(tx) => tx.to_proto(),
            DeployAccount(tx) => tx.to_proto(),
        }
    }
}

impl ModelExt for models::InvokeTransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionReceiptMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            actual_fee: self.actual_fee.to_proto().into(),
            execution_resources: self.execution_resources.to_proto().into(),
            execution_result: self.execution_result.to_proto().into(),
            transaction_index: u32::MAX,
        };

        let inner = starknet::InvokeTransactionReceipt {};

        starknet::TransactionReceipt {
            filter_ids: Vec::default(),
            meta: meta.into(),
            receipt: Some(starknet::transaction_receipt::Receipt::Invoke(inner)),
        }
    }
}

impl ModelExt for models::L1HandlerTransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionReceiptMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            actual_fee: self.actual_fee.to_proto().into(),
            execution_resources: self.execution_resources.to_proto().into(),
            execution_result: self.execution_result.to_proto().into(),
            transaction_index: u32::MAX,
        };

        let inner = starknet::L1HandlerTransactionReceipt {
            message_hash: self.message_hash.as_bytes().to_vec(),
        };

        starknet::TransactionReceipt {
            filter_ids: Vec::default(),
            meta: meta.into(),
            receipt: Some(starknet::transaction_receipt::Receipt::L1Handler(inner)),
        }
    }
}

impl ModelExt for models::DeclareTransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionReceiptMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            actual_fee: self.actual_fee.to_proto().into(),
            execution_resources: self.execution_resources.to_proto().into(),
            execution_result: self.execution_result.to_proto().into(),
            transaction_index: u32::MAX,
        };

        let inner = starknet::DeclareTransactionReceipt {};

        starknet::TransactionReceipt {
            filter_ids: Vec::default(),
            meta: meta.into(),
            receipt: Some(starknet::transaction_receipt::Receipt::Declare(inner)),
        }
    }
}

impl ModelExt for models::DeployTransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionReceiptMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            actual_fee: self.actual_fee.to_proto().into(),
            execution_resources: self.execution_resources.to_proto().into(),
            execution_result: self.execution_result.to_proto().into(),
            transaction_index: u32::MAX,
        };

        let inner = starknet::DeployTransactionReceipt {
            contract_address: self.contract_address.to_proto().into(),
        };

        starknet::TransactionReceipt {
            filter_ids: Vec::default(),
            meta: meta.into(),
            receipt: Some(starknet::transaction_receipt::Receipt::Deploy(inner)),
        }
    }
}

impl ModelExt for models::DeployAccountTransactionReceipt {
    type Proto = starknet::TransactionReceipt;

    fn to_proto(&self) -> Self::Proto {
        let meta = starknet::TransactionReceiptMeta {
            transaction_hash: self.transaction_hash.to_proto().into(),
            actual_fee: self.actual_fee.to_proto().into(),
            execution_resources: self.execution_resources.to_proto().into(),
            execution_result: self.execution_result.to_proto().into(),
            transaction_index: u32::MAX,
        };

        let inner = starknet::DeployAccountTransactionReceipt {
            contract_address: self.contract_address.to_proto().into(),
        };

        starknet::TransactionReceipt {
            filter_ids: Vec::default(),
            meta: meta.into(),
            receipt: Some(starknet::transaction_receipt::Receipt::DeployAccount(inner)),
        }
    }
}

impl ModelExt for models::Event {
    type Proto = starknet::Event;

    fn to_proto(&self) -> Self::Proto {
        starknet::Event {
            filter_ids: Vec::default(),
            from_address: self.from_address.to_proto().into(),
            keys: self.keys.iter().map(ModelExt::to_proto).collect(),
            data: self.data.iter().map(ModelExt::to_proto).collect(),
            event_index: u32::MAX,
            transaction_index: u32::MAX,
            transaction_hash: None,
            transaction_status: 0,
        }
    }
}

impl ModelExt for models::MsgToL1 {
    type Proto = starknet::MessageToL1;

    fn to_proto(&self) -> Self::Proto {
        starknet::MessageToL1 {
            filter_ids: Vec::default(),
            from_address: self.from_address.to_proto().into(),
            to_address: self.to_address.to_proto().into(),
            payload: self.payload.iter().map(ModelExt::to_proto).collect(),
            message_index: u32::MAX,
            transaction_index: u32::MAX,
            transaction_hash: None,
            transaction_status: 0,
        }
    }
}

impl ModelExt for models::ResourceBoundsMapping {
    type Proto = starknet::ResourceBoundsMapping;

    fn to_proto(&self) -> Self::Proto {
        starknet::ResourceBoundsMapping {
            l1_gas: self.l1_gas.to_proto().into(),
            l2_gas: self.l2_gas.to_proto().into(),
        }
    }
}

impl ModelExt for models::ResourceBounds {
    type Proto = starknet::ResourceBounds;

    fn to_proto(&self) -> Self::Proto {
        starknet::ResourceBounds {
            max_amount: self.max_amount,
            max_price_per_unit: self.max_price_per_unit.to_proto().into(),
        }
    }
}

impl ModelExt for u128 {
    type Proto = starknet::Uint128;

    fn to_proto(&self) -> Self::Proto {
        starknet::Uint128::from_bytes(&self.to_be_bytes())
    }
}

impl ModelExt for models::DataAvailabilityMode {
    type Proto = i32;

    fn to_proto(&self) -> Self::Proto {
        use models::DataAvailabilityMode::*;

        match self {
            L1 => starknet::DataAvailabilityMode::L1 as i32,
            L2 => starknet::DataAvailabilityMode::L2 as i32,
        }
    }
}

impl ModelExt for models::ExecutionResources {
    type Proto = starknet::ExecutionResources;

    fn to_proto(&self) -> Self::Proto {
        starknet::ExecutionResources {
            computation: self.computation_resources.to_proto().into(),
            data_availability: self.data_resources.to_proto().into(),
        }
    }
}

impl ModelExt for models::ComputationResources {
    type Proto = starknet::ComputationResources;

    fn to_proto(&self) -> Self::Proto {
        starknet::ComputationResources {
            steps: self.steps,
            memory_holes: self.memory_holes,
            range_check_builtin_applications: self.range_check_builtin_applications,
            pedersen_builtin_applications: self.pedersen_builtin_applications,
            poseidon_builtin_applications: self.poseidon_builtin_applications,
            ec_op_builtin_applications: self.ec_op_builtin_applications,
            ecdsa_builtin_applications: self.ecdsa_builtin_applications,
            bitwise_builtin_applications: self.bitwise_builtin_applications,
            keccak_builtin_applications: self.keccak_builtin_applications,
            segment_arena_builtin: self.segment_arena_builtin,
        }
    }
}

impl ModelExt for models::DataResources {
    type Proto = starknet::DataAvailabilityResources;

    fn to_proto(&self) -> Self::Proto {
        starknet::DataAvailabilityResources {
            l1_gas: self.data_availability.l1_gas,
            l1_data_gas: self.data_availability.l1_data_gas,
        }
    }
}

impl ModelExt for models::FeePayment {
    type Proto = starknet::FeePayment;

    fn to_proto(&self) -> Self::Proto {
        starknet::FeePayment {
            amount: self.amount.to_proto().into(),
            unit: self.unit.to_proto().into(),
        }
    }
}

impl ModelExt for models::PriceUnit {
    type Proto = starknet::PriceUnit;

    fn to_proto(&self) -> Self::Proto {
        use models::PriceUnit::*;

        match self {
            Fri => starknet::PriceUnit::Fri,
            Wei => starknet::PriceUnit::Wei,
        }
    }
}

impl ModelExt for models::ExecutionResult {
    type Proto = starknet::transaction_receipt_meta::ExecutionResult;

    fn to_proto(&self) -> Self::Proto {
        use models::ExecutionResult::*;

        match self {
            Succeeded => starknet::transaction_receipt_meta::ExecutionResult::Succeeded(
                starknet::ExecutionSucceeded {},
            ),
            Reverted { reason } => starknet::transaction_receipt_meta::ExecutionResult::Reverted(
                starknet::ExecutionReverted {
                    reason: reason.clone(),
                },
            ),
        }
    }
}
