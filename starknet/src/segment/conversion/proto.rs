use apibara_dna_protocol::starknet;

use crate::segment::store;

impl From<store::BlockHeader> for starknet::BlockHeader {
    fn from(value: store::BlockHeader) -> Self {
        let block_hash = (&value.block_hash).into();
        let parent_block_hash = (&value.parent_block_hash).into();
        let sequencer_address = (&value.sequencer_address).into();
        let new_root = (&value.new_root).into();
        let timestamp = prost_types::Timestamp {
            seconds: value.timestamp as i64,
            nanos: 0,
        };
        let l1_gas_price = value.l1_gas_price.into();
        let l1_data_gas_price = value.l1_data_gas_price.into();
        let l1_data_availability_mode: starknet::L1DataAvailabilityMode =
            (&value.l1_data_availability_mode).into();

        starknet::BlockHeader {
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
            block_number: value.block_number,
            sequencer_address: Some(sequencer_address),
            new_root: Some(new_root),
            timestamp: Some(timestamp),
            starknet_version: value.starknet_version.clone(),
            l1_gas_price: Some(l1_gas_price),
            l1_data_gas_price: Some(l1_data_gas_price),
            l1_data_availability_mode: l1_data_availability_mode as i32,
        }
    }
}

impl From<store::Event> for starknet::Event {
    fn from(value: store::Event) -> Self {
        let from_address = (&value.from_address).into();
        let keys = value
            .keys
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let data = value
            .data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let transaction_hash = (&value.transaction_hash).into();

        starknet::Event {
            from_address: Some(from_address),
            keys,
            data,
            event_index: value.event_index,
            transaction_hash: Some(transaction_hash),
            transaction_index: value.transaction_index,
            transaction_reverted: value.transaction_reverted,
        }
    }
}

impl From<store::MessageToL1> for starknet::MessageToL1 {
    fn from(value: store::MessageToL1) -> Self {
        let from_address = (&value.from_address).into();
        let to_address = (&value.to_address).into();
        let payload = value
            .payload
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let transaction_hash = (&value.transaction_hash).into();

        starknet::MessageToL1 {
            from_address: Some(from_address),
            to_address: Some(to_address),
            payload,
            message_index: value.message_index,
            transaction_hash: Some(transaction_hash),
            transaction_index: value.transaction_index,
            transaction_reverted: value.transaction_reverted,
        }
    }
}

impl From<store::Transaction> for starknet::Transaction {
    fn from(value: store::Transaction) -> Self {
        use starknet::transaction::Transaction::*;
        use store::Transaction::*;

        let meta = value.meta().into();
        let inner = match value {
            InvokeTransactionV0(inner) => InvokeV0(inner.into()),
            InvokeTransactionV1(inner) => InvokeV1(inner.into()),
            InvokeTransactionV3(inner) => InvokeV3(inner.into()),
            L1HandlerTransaction(inner) => L1Handler(inner.into()),
            DeployTransaction(inner) => Deploy(inner.into()),
            DeclareTransactionV0(inner) => DeclareV0(inner.into()),
            DeclareTransactionV1(inner) => DeclareV1(inner.into()),
            DeclareTransactionV2(inner) => DeclareV2(inner.into()),
            DeclareTransactionV3(inner) => DeclareV3(inner.into()),
            DeployAccountTransactionV1(inner) => DeployAccountV1(inner.into()),
            DeployAccountTransactionV3(inner) => DeployAccountV3(inner.into()),
        };

        starknet::Transaction {
            meta: Some(meta),
            transaction: Some(inner),
        }
    }
}

impl From<&store::TransactionMeta> for starknet::TransactionMeta {
    fn from(value: &store::TransactionMeta) -> Self {
        let transaction_hash = (&value.transaction_hash).into();
        starknet::TransactionMeta {
            transaction_index: value.transaction_index,
            transaction_hash: Some(transaction_hash),
            transaction_reverted: value.transaction_reverted,
        }
    }
}

impl From<store::InvokeTransactionV0> for starknet::InvokeTransactionV0 {
    fn from(value: store::InvokeTransactionV0) -> Self {
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let contract_address = (&value.contract_address).into();
        let entry_point_selector = (&value.entry_point_selector).into();
        let calldata = value
            .calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();

        starknet::InvokeTransactionV0 {
            max_fee: Some(max_fee),
            signature,
            contract_address: Some(contract_address),
            entry_point_selector: Some(entry_point_selector),
            calldata,
        }
    }
}

impl From<store::InvokeTransactionV1> for starknet::InvokeTransactionV1 {
    fn from(value: store::InvokeTransactionV1) -> Self {
        let sender_address = (&value.sender_address).into();
        let calldata = value
            .calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();

        starknet::InvokeTransactionV1 {
            sender_address: Some(sender_address),
            calldata,
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
        }
    }
}

impl From<store::InvokeTransactionV3> for starknet::InvokeTransactionV3 {
    fn from(value: store::InvokeTransactionV3) -> Self {
        let sender_address = (&value.sender_address).into();
        let calldata = value
            .calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let resource_bounds = (&value.resource_bounds).into();
        let paymaster_data = value
            .paymaster_data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let account_deployment_data = value
            .account_deployment_data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.nonce_data_availability_mode);
        let fee_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.fee_data_availability_mode);

        starknet::InvokeTransactionV3 {
            sender_address: Some(sender_address),
            calldata,
            signature,
            nonce: Some(nonce),
            resource_bounds: Some(resource_bounds),
            tip: value.tip,
            paymaster_data,
            account_deployment_data,
            nonce_data_availability_mode: nonce_data_availability_mode as i32,
            fee_data_availability_mode: fee_data_availability_mode as i32,
        }
    }
}

impl From<store::L1HandlerTransaction> for starknet::L1HandlerTransaction {
    fn from(value: store::L1HandlerTransaction) -> Self {
        let contract_address = (&value.contract_address).into();
        let entry_point_selector = (&value.entry_point_selector).into();
        let calldata = value
            .calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();

        starknet::L1HandlerTransaction {
            nonce: value.nonce,
            contract_address: Some(contract_address),
            entry_point_selector: Some(entry_point_selector),
            calldata,
        }
    }
}

impl From<store::DeployTransaction> for starknet::DeployTransaction {
    fn from(value: store::DeployTransaction) -> Self {
        let contract_address_salt = (&value.contract_address_salt).into();
        let constructor_calldata = value
            .constructor_calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let class_hash = (&value.class_hash).into();

        starknet::DeployTransaction {
            contract_address_salt: Some(contract_address_salt),
            constructor_calldata,
            class_hash: Some(class_hash),
        }
    }
}

impl From<store::DeclareTransactionV0> for starknet::DeclareTransactionV0 {
    fn from(value: store::DeclareTransactionV0) -> Self {
        let sender_address = (&value.sender_address).into();
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let class_hash = (&value.class_hash).into();

        starknet::DeclareTransactionV0 {
            sender_address: Some(sender_address),
            max_fee: Some(max_fee),
            signature,
            class_hash: Some(class_hash),
        }
    }
}

impl From<store::DeclareTransactionV1> for starknet::DeclareTransactionV1 {
    fn from(value: store::DeclareTransactionV1) -> Self {
        let sender_address = (&value.sender_address).into();
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let class_hash = (&value.class_hash).into();

        starknet::DeclareTransactionV1 {
            sender_address: Some(sender_address),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            class_hash: Some(class_hash),
        }
    }
}

impl From<store::DeclareTransactionV2> for starknet::DeclareTransactionV2 {
    fn from(value: store::DeclareTransactionV2) -> Self {
        let sender_address = (&value.sender_address).into();
        let compiled_class_hash = (&value.compiled_class_hash).into();
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let class_hash = (&value.class_hash).into();

        starknet::DeclareTransactionV2 {
            sender_address: Some(sender_address),
            compiled_class_hash: Some(compiled_class_hash),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            class_hash: Some(class_hash),
        }
    }
}

impl From<store::DeclareTransactionV3> for starknet::DeclareTransactionV3 {
    fn from(value: store::DeclareTransactionV3) -> Self {
        let sender_address = (&value.sender_address).into();
        let compiled_class_hash = (&value.compiled_class_hash).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let class_hash = (&value.class_hash).into();
        let resource_bounds = (&value.resource_bounds).into();
        let paymaster_data = value
            .paymaster_data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let account_deployment_data = value
            .account_deployment_data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.nonce_data_availability_mode);
        let fee_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.fee_data_availability_mode);

        starknet::DeclareTransactionV3 {
            sender_address: Some(sender_address),
            compiled_class_hash: Some(compiled_class_hash),
            signature,
            nonce: Some(nonce),
            class_hash: Some(class_hash),
            resource_bounds: Some(resource_bounds),
            tip: value.tip,
            paymaster_data,
            account_deployment_data,
            nonce_data_availability_mode: nonce_data_availability_mode as i32,
            fee_data_availability_mode: fee_data_availability_mode as i32,
        }
    }
}

impl From<store::DeployAccountTransactionV1> for starknet::DeployAccountTransactionV1 {
    fn from(value: store::DeployAccountTransactionV1) -> Self {
        let max_fee = (&value.max_fee).into();
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let contract_address_salt = (&value.contract_address_salt).into();
        let constructor_calldata = value
            .constructor_calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let class_hash = (&value.class_hash).into();

        starknet::DeployAccountTransactionV1 {
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            contract_address_salt: Some(contract_address_salt),
            constructor_calldata,
            class_hash: Some(class_hash),
        }
    }
}

impl From<store::DeployAccountTransactionV3> for starknet::DeployAccountTransactionV3 {
    fn from(value: store::DeployAccountTransactionV3) -> Self {
        let signature = value
            .signature
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce = (&value.nonce).into();
        let contract_address_salt = (&value.contract_address_salt).into();
        let constructor_calldata = value
            .constructor_calldata
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let class_hash = (&value.class_hash).into();
        let resource_bounds = (&value.resource_bounds).into();
        let paymaster_data = value
            .paymaster_data
            .iter()
            .map(starknet::FieldElement::from)
            .collect();
        let nonce_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.nonce_data_availability_mode);
        let fee_data_availability_mode =
            starknet::DataAvailabilityMode::from(&value.fee_data_availability_mode);

        starknet::DeployAccountTransactionV3 {
            signature,
            nonce: Some(nonce),
            contract_address_salt: Some(contract_address_salt),
            constructor_calldata,
            class_hash: Some(class_hash),
            resource_bounds: Some(resource_bounds),
            tip: value.tip,
            paymaster_data,
            nonce_data_availability_mode: nonce_data_availability_mode as i32,
            fee_data_availability_mode: fee_data_availability_mode as i32,
        }
    }
}

impl From<store::TransactionReceipt> for starknet::TransactionReceipt {
    fn from(value: store::TransactionReceipt) -> Self {
        use starknet::transaction_receipt::Receipt;
        use store::TransactionReceipt::*;

        let meta = value.meta().into();
        let inner = match value {
            Invoke(inner) => Receipt::Invoke(inner.into()),
            L1Handler(inner) => Receipt::L1Handler(inner.into()),
            Deploy(inner) => Receipt::Deploy(inner.into()),
            Declare(inner) => Receipt::Declare(inner.into()),
            DeployAccount(inner) => Receipt::DeployAccount(inner.into()),
        };

        starknet::TransactionReceipt {
            meta: Some(meta),
            receipt: Some(inner),
        }
    }
}

impl From<&store::TransactionReceiptMeta> for starknet::TransactionReceiptMeta {
    fn from(value: &store::TransactionReceiptMeta) -> Self {
        let transaction_hash = (&value.transaction_hash).into();
        let actual_fee = (&value.actual_fee).into();
        let execution_resources = (&value.execution_resources).into();
        let execution_result = (&value.execution_result).into();

        starknet::TransactionReceiptMeta {
            transaction_index: value.transaction_index,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            execution_resources: Some(execution_resources),
            execution_result: Some(execution_result),
        }
    }
}

impl From<store::InvokeTransactionReceipt> for starknet::InvokeTransactionReceipt {
    fn from(_value: store::InvokeTransactionReceipt) -> Self {
        starknet::InvokeTransactionReceipt {}
    }
}

impl From<store::L1HandlerTransactionReceipt> for starknet::L1HandlerTransactionReceipt {
    fn from(value: store::L1HandlerTransactionReceipt) -> Self {
        starknet::L1HandlerTransactionReceipt {
            message_hash: value.message_hash.clone(),
        }
    }
}

impl From<store::DeclareTransactionReceipt> for starknet::DeclareTransactionReceipt {
    fn from(_value: store::DeclareTransactionReceipt) -> Self {
        starknet::DeclareTransactionReceipt {}
    }
}

impl From<store::DeployTransactionReceipt> for starknet::DeployTransactionReceipt {
    fn from(value: store::DeployTransactionReceipt) -> Self {
        let contract_address = (&value.contract_address).into();

        starknet::DeployTransactionReceipt {
            contract_address: Some(contract_address),
        }
    }
}

impl From<store::DeployAccountTransactionReceipt> for starknet::DeployAccountTransactionReceipt {
    fn from(value: store::DeployAccountTransactionReceipt) -> Self {
        let contract_address = (&value.contract_address).into();

        starknet::DeployAccountTransactionReceipt {
            contract_address: Some(contract_address),
        }
    }
}

impl From<store::ResourcePrice> for starknet::ResourcePrice {
    fn from(value: store::ResourcePrice) -> Self {
        let price_in_fri = (&value.price_in_fri).into();
        let price_in_wei = (&value.price_in_wei).into();

        starknet::ResourcePrice {
            price_in_fri: Some(price_in_fri),
            price_in_wei: Some(price_in_wei),
        }
    }
}

impl From<&store::FeePayment> for starknet::FeePayment {
    fn from(value: &store::FeePayment) -> Self {
        let amount = (&value.amount).into();
        let unit = starknet::PriceUnit::from(&value.unit);

        starknet::FeePayment {
            amount: Some(amount),
            unit: unit as i32,
        }
    }
}

impl From<&store::PriceUnit> for starknet::PriceUnit {
    fn from(value: &store::PriceUnit) -> Self {
        use store::PriceUnit::*;

        match value {
            Fri => starknet::PriceUnit::Fri,
            Wei => starknet::PriceUnit::Wei,
        }
    }
}

impl From<&store::ExecutionResources> for starknet::ExecutionResources {
    fn from(value: &store::ExecutionResources) -> Self {
        let computation = (&value.computation).into();
        let data = (&value.data_availability).into();

        starknet::ExecutionResources {
            computation: Some(computation),
            data_availability: Some(data),
        }
    }
}

impl From<&store::ComputationResources> for starknet::ComputationResources {
    fn from(value: &store::ComputationResources) -> Self {
        starknet::ComputationResources {
            steps: value.steps,
            memory_holes: value.memory_holes,
            range_check_builtin_applications: value.range_check_builtin_applications,
            pedersen_builtin_applications: value.pedersen_builtin_applications,
            poseidon_builtin_applications: value.poseidon_builtin_applications,
            ec_op_builtin_applications: value.ec_op_builtin_applications,
            ecdsa_builtin_applications: value.ecdsa_builtin_applications,
            bitwise_builtin_applications: value.bitwise_builtin_applications,
            keccak_builtin_applications: value.keccak_builtin_applications,
            segment_arena_builtin: value.segment_arena_builtin,
        }
    }
}

impl From<&store::DataResources> for starknet::DataAvailabilityResources {
    fn from(value: &store::DataResources) -> Self {
        starknet::DataAvailabilityResources {
            l1_gas: value.l1_gas,
            l1_data_gas: value.l1_data_gas,
        }
    }
}

impl From<&store::ExecutionResult> for starknet::transaction_receipt_meta::ExecutionResult {
    fn from(value: &store::ExecutionResult) -> Self {
        use starknet::transaction_receipt_meta::ExecutionResult;

        match value {
            &store::ExecutionResult::Succeeded => {
                ExecutionResult::Succeeded(starknet::ExecutionSucceeded {})
            }
            &store::ExecutionResult::Reverted { ref reason } => {
                ExecutionResult::Reverted(starknet::ExecutionReverted {
                    reason: reason.clone(),
                })
            }
        }
    }
}

impl From<&store::ResourceBoundsMapping> for starknet::ResourceBoundsMapping {
    fn from(value: &store::ResourceBoundsMapping) -> Self {
        let l1_gas = (&value.l1_gas).into();
        let l2_gas = (&value.l2_gas).into();

        starknet::ResourceBoundsMapping {
            l1_gas: Some(l1_gas),
            l2_gas: Some(l2_gas),
        }
    }
}

impl From<&store::ResourceBounds> for starknet::ResourceBounds {
    fn from(value: &store::ResourceBounds) -> Self {
        let max_price_per_unit = {
            let b = value.max_price_per_unit.to_be_bytes();
            let low = u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            let high = u64::from_be_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]);
            starknet::Uint128 { low, high }
        };

        starknet::ResourceBounds {
            max_amount: value.max_amount,
            max_price_per_unit: Some(max_price_per_unit),
        }
    }
}

impl From<&store::DataAvailabilityMode> for starknet::DataAvailabilityMode {
    fn from(value: &store::DataAvailabilityMode) -> Self {
        use store::DataAvailabilityMode::*;
        match value {
            L1 => starknet::DataAvailabilityMode::L1,
            L2 => starknet::DataAvailabilityMode::L2,
        }
    }
}

impl From<&store::L1DataAvailabilityMode> for starknet::L1DataAvailabilityMode {
    fn from(value: &store::L1DataAvailabilityMode) -> Self {
        use store::L1DataAvailabilityMode::*;
        match value {
            Blob => starknet::L1DataAvailabilityMode::Blob,
            Calldata => starknet::L1DataAvailabilityMode::Calldata,
        }
    }
}

impl From<&store::FieldElement> for starknet::FieldElement {
    fn from(value: &store::FieldElement) -> Self {
        starknet::FieldElement::from_bytes(&value.0)
    }
}

impl From<starknet::FieldElement> for store::FieldElement {
    fn from(value: starknet::FieldElement) -> Self {
        store::FieldElement(value.to_bytes())
    }
}
