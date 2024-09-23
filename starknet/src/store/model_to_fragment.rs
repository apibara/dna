//! Convert models to fragments.

use crate::{provider::models, store::fragment as store};

impl From<&models::FieldElement> for store::FieldElement {
    fn from(felt: &models::FieldElement) -> Self {
        store::FieldElement(felt.to_bytes_be())
    }
}

impl From<models::FieldElement> for store::FieldElement {
    fn from(felt: models::FieldElement) -> Self {
        (&felt).into()
    }
}

impl From<&models::ResourcePrice> for store::ResourcePrice {
    fn from(value: &models::ResourcePrice) -> Self {
        store::ResourcePrice {
            price_in_fri: value.price_in_fri.into(),
            price_in_wei: value.price_in_wei.into(),
        }
    }
}
impl From<&models::ResourceBoundsMapping> for store::ResourceBoundsMapping {
    fn from(value: &models::ResourceBoundsMapping) -> Self {
        store::ResourceBoundsMapping {
            l1_gas: (&value.l1_gas).into(),
            l2_gas: (&value.l2_gas).into(),
        }
    }
}

impl From<&models::ResourceBounds> for store::ResourceBounds {
    fn from(value: &models::ResourceBounds) -> Self {
        store::ResourceBounds {
            max_amount: value.max_amount,
            max_price_per_unit: value.max_price_per_unit,
        }
    }
}

impl From<models::DataAvailabilityMode> for store::DataAvailabilityMode {
    fn from(value: models::DataAvailabilityMode) -> Self {
        use models::DataAvailabilityMode::*;
        match value {
            L1 => store::DataAvailabilityMode::L1,
            L2 => store::DataAvailabilityMode::L2,
        }
    }
}

impl From<&models::FeePayment> for store::FeePayment {
    fn from(value: &models::FeePayment) -> Self {
        store::FeePayment {
            amount: value.amount.into(),
            unit: value.unit.into(),
        }
    }
}

impl From<models::PriceUnit> for store::PriceUnit {
    fn from(value: models::PriceUnit) -> Self {
        use models::PriceUnit::*;
        match value {
            Fri => store::PriceUnit::Fri,
            Wei => store::PriceUnit::Wei,
        }
    }
}

impl From<&models::ExecutionResources> for store::ExecutionResources {
    fn from(value: &models::ExecutionResources) -> Self {
        store::ExecutionResources {
            computation: (&value.computation_resources).into(),
            data_availability: (&value.data_resources).into(),
        }
    }
}

impl From<&models::ComputationResources> for store::ComputationResources {
    fn from(value: &models::ComputationResources) -> Self {
        store::ComputationResources {
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

impl From<&models::DataResources> for store::DataResources {
    fn from(value: &models::DataResources) -> Self {
        store::DataResources {
            l1_data_gas: value.data_availability.l1_data_gas,
            l1_gas: value.data_availability.l1_gas,
        }
    }
}

impl From<&models::ExecutionResult> for store::ExecutionResult {
    fn from(value: &models::ExecutionResult) -> Self {
        use models::ExecutionResult::*;
        match value {
            Succeeded => store::ExecutionResult::Succeeded,
            Reverted { reason } => store::ExecutionResult::Reverted {
                reason: reason.clone(),
            },
        }
    }
}

impl From<models::L1DataAvailabilityMode> for store::L1DataAvailabilityMode {
    fn from(value: models::L1DataAvailabilityMode) -> Self {
        use models::L1DataAvailabilityMode::*;
        match value {
            Blob => store::L1DataAvailabilityMode::Blob,
            Calldata => store::L1DataAvailabilityMode::Calldata,
        }
    }
}

impl From<&models::BlockWithReceipts> for store::BlockHeader {
    fn from(block: &models::BlockWithReceipts) -> Self {
        store::BlockHeader {
            block_hash: block.block_hash.into(),
            parent_block_hash: block.parent_hash.into(),
            block_number: block.block_number,
            sequencer_address: block.sequencer_address.into(),
            new_root: block.new_root.into(),
            timestamp: block.timestamp,
            starknet_version: block.starknet_version.clone(),
            l1_gas_price: (&block.l1_gas_price).into(),
            l1_data_gas_price: (&block.l1_data_gas_price).into(),
            l1_data_availability_mode: block.l1_da_mode.into(),
        }
    }
}

impl From<&models::Transaction> for store::Transaction {
    fn from(tx: &models::Transaction) -> Self {
        use models::Transaction::*;
        match tx {
            Invoke(tx) => tx.into(),
            L1Handler(tx) => tx.into(),
            Deploy(tx) => tx.into(),
            Declare(tx) => tx.into(),
            DeployAccount(tx) => tx.into(),
        }
    }
}

impl From<&models::InvokeTransaction> for store::Transaction {
    fn from(tx: &models::InvokeTransaction) -> Self {
        use models::InvokeTransaction::*;
        match tx {
            V0(tx) => tx.into(),
            V1(tx) => tx.into(),
            V3(tx) => tx.into(),
        }
    }
}

impl From<&models::InvokeTransactionV0> for store::Transaction {
    fn from(tx: &models::InvokeTransactionV0) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::InvokeTransactionV0(store::InvokeTransactionV0 {
            meta,
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            contract_address: tx.contract_address.into(),
            entry_point_selector: tx.entry_point_selector.into(),
            calldata: tx.calldata.iter().map(store::FieldElement::from).collect(),
        })
    }
}

impl From<&models::InvokeTransactionV1> for store::Transaction {
    fn from(tx: &models::InvokeTransactionV1) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::InvokeTransactionV1(store::InvokeTransactionV1 {
            meta,
            sender_address: tx.sender_address.into(),
            calldata: tx.calldata.iter().map(store::FieldElement::from).collect(),
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
        })
    }
}

impl From<&models::InvokeTransactionV3> for store::Transaction {
    fn from(tx: &models::InvokeTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::InvokeTransactionV3(store::InvokeTransactionV3 {
            meta,
            sender_address: tx.sender_address.into(),
            calldata: tx.calldata.iter().map(store::FieldElement::from).collect(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            resource_bounds: (&tx.resource_bounds).into(),
            tip: tx.tip,
            paymaster_data: tx
                .paymaster_data
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            account_deployment_data: tx
                .account_deployment_data
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
            fee_data_availability_mode: tx.fee_data_availability_mode.into(),
        })
    }
}

impl From<&models::L1HandlerTransaction> for store::Transaction {
    fn from(tx: &models::L1HandlerTransaction) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::L1HandlerTransaction(store::L1HandlerTransaction {
            meta,
            nonce: tx.nonce,
            contract_address: tx.contract_address.into(),
            entry_point_selector: tx.entry_point_selector.into(),
            calldata: tx.calldata.iter().map(store::FieldElement::from).collect(),
        })
    }
}

impl From<&models::DeployTransaction> for store::Transaction {
    fn from(tx: &models::DeployTransaction) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeployTransaction(store::DeployTransaction {
            meta,
            contract_address_salt: tx.contract_address_salt.into(),
            constructor_calldata: tx
                .constructor_calldata
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            class_hash: tx.class_hash.into(),
        })
    }
}

impl From<&models::DeclareTransaction> for store::Transaction {
    fn from(tx: &models::DeclareTransaction) -> Self {
        use models::DeclareTransaction::*;
        match tx {
            V0(tx) => tx.into(),
            V1(tx) => tx.into(),
            V2(tx) => tx.into(),
            V3(tx) => tx.into(),
        }
    }
}

impl From<&models::DeclareTransactionV0> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV0) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeclareTransactionV0(store::DeclareTransactionV0 {
            meta,
            sender_address: tx.sender_address.into(),
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            class_hash: tx.class_hash.into(),
        })
    }
}

impl From<&models::DeclareTransactionV1> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV1) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeclareTransactionV1(store::DeclareTransactionV1 {
            meta,
            sender_address: tx.sender_address.into(),
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            class_hash: tx.class_hash.into(),
        })
    }
}

impl From<&models::DeclareTransactionV2> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV2) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeclareTransactionV2(store::DeclareTransactionV2 {
            meta,
            sender_address: tx.sender_address.into(),
            compiled_class_hash: tx.compiled_class_hash.into(),
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            class_hash: tx.class_hash.into(),
        })
    }
}

impl From<&models::DeclareTransactionV3> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeclareTransactionV3(store::DeclareTransactionV3 {
            meta,
            sender_address: tx.sender_address.into(),
            compiled_class_hash: tx.compiled_class_hash.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            class_hash: tx.class_hash.into(),
            resource_bounds: (&tx.resource_bounds).into(),
            tip: tx.tip,
            paymaster_data: tx
                .paymaster_data
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            account_deployment_data: tx
                .account_deployment_data
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
            fee_data_availability_mode: tx.fee_data_availability_mode.into(),
        })
    }
}

impl From<&models::DeployAccountTransaction> for store::Transaction {
    fn from(tx: &models::DeployAccountTransaction) -> Self {
        use models::DeployAccountTransaction::*;
        match tx {
            V1(tx) => tx.into(),
            V3(tx) => tx.into(),
        }
    }
}

impl From<&models::DeployAccountTransactionV1> for store::Transaction {
    fn from(tx: &models::DeployAccountTransactionV1) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeployAccountTransactionV1(store::DeployAccountTransactionV1 {
            meta,
            max_fee: tx.max_fee.into(),
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            contract_address_salt: tx.contract_address_salt.into(),
            constructor_calldata: tx
                .constructor_calldata
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            class_hash: tx.class_hash.into(),
        })
    }
}

impl From<&models::DeployAccountTransactionV3> for store::Transaction {
    fn from(tx: &models::DeployAccountTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
            transaction_reverted: false,
        };

        store::Transaction::DeployAccountTransactionV3(store::DeployAccountTransactionV3 {
            meta,
            signature: tx.signature.iter().map(store::FieldElement::from).collect(),
            nonce: tx.nonce.into(),
            contract_address_salt: tx.contract_address_salt.into(),
            constructor_calldata: tx
                .constructor_calldata
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            class_hash: tx.class_hash.into(),
            resource_bounds: (&tx.resource_bounds).into(),
            tip: tx.tip,
            paymaster_data: tx
                .paymaster_data
                .iter()
                .map(store::FieldElement::from)
                .collect(),
            nonce_data_availability_mode: tx.nonce_data_availability_mode.into(),
            fee_data_availability_mode: tx.fee_data_availability_mode.into(),
        })
    }
}

impl From<&models::TransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::TransactionReceipt) -> Self {
        use models::TransactionReceipt::*;
        match receipt {
            Invoke(receipt) => receipt.into(),
            L1Handler(receipt) => receipt.into(),
            Deploy(receipt) => receipt.into(),
            Declare(receipt) => receipt.into(),
            DeployAccount(receipt) => receipt.into(),
        }
    }
}

impl From<&models::InvokeTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::InvokeTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
            actual_fee: (&receipt.actual_fee).into(),
            execution_resources: (&receipt.execution_resources).into(),
            execution_result: (&receipt.execution_result).into(),
        };

        store::TransactionReceipt::Invoke(store::InvokeTransactionReceipt { meta })
    }
}

impl From<&models::L1HandlerTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::L1HandlerTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
            actual_fee: (&receipt.actual_fee).into(),
            execution_resources: (&receipt.execution_resources).into(),
            execution_result: (&receipt.execution_result).into(),
        };

        store::TransactionReceipt::L1Handler(store::L1HandlerTransactionReceipt {
            meta,
            message_hash: receipt.message_hash.as_bytes().to_vec(),
        })
    }
}

impl From<&models::DeployTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeployTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
            actual_fee: (&receipt.actual_fee).into(),
            execution_resources: (&receipt.execution_resources).into(),
            execution_result: (&receipt.execution_result).into(),
        };

        store::TransactionReceipt::Deploy(store::DeployTransactionReceipt {
            meta,
            contract_address: receipt.contract_address.into(),
        })
    }
}

impl From<&models::DeclareTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeclareTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
            actual_fee: (&receipt.actual_fee).into(),
            execution_resources: (&receipt.execution_resources).into(),
            execution_result: (&receipt.execution_result).into(),
        };

        store::TransactionReceipt::Declare(store::DeclareTransactionReceipt { meta })
    }
}

impl From<&models::DeployAccountTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeployAccountTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
            actual_fee: (&receipt.actual_fee).into(),
            execution_resources: (&receipt.execution_resources).into(),
            execution_result: (&receipt.execution_result).into(),
        };

        store::TransactionReceipt::DeployAccount(store::DeployAccountTransactionReceipt {
            meta,
            contract_address: receipt.contract_address.into(),
        })
    }
}

impl From<&models::Event> for store::Event {
    fn from(event: &models::Event) -> Self {
        store::Event {
            address: event.from_address.into(),
            keys: event.keys.iter().map(store::FieldElement::from).collect(),
            data: event.data.iter().map(store::FieldElement::from).collect(),

            event_index: u32::MAX,
            transaction_index: u32::MAX,
            transaction_hash: store::FieldElement::default(),
            transaction_reverted: false,
        }
    }
}

impl From<&models::MsgToL1> for store::MessageToL1 {
    fn from(message: &models::MsgToL1) -> Self {
        store::MessageToL1 {
            from_address: message.from_address.into(),
            to_address: message.to_address.into(),
            payload: message
                .payload
                .iter()
                .map(store::FieldElement::from)
                .collect(),

            message_index: u32::MAX,
            transaction_index: u32::MAX,
            transaction_hash: store::FieldElement::default(),
            transaction_reverted: false,
        }
    }
}
