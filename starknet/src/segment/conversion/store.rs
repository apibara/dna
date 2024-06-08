use crate::{ingestion::models, segment::store};

use super::model::TransactionReceiptExt;

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
        };

        store::Transaction::InvokeTransactionV0(store::InvokeTransactionV0 { meta })
    }
}

impl From<&models::InvokeTransactionV1> for store::Transaction {
    fn from(tx: &models::InvokeTransactionV1) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::InvokeTransactionV1(store::InvokeTransactionV1 { meta })
    }
}

impl From<&models::InvokeTransactionV3> for store::Transaction {
    fn from(tx: &models::InvokeTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::InvokeTransactionV3(store::InvokeTransactionV3 { meta })
    }
}

impl From<&models::L1HandlerTransaction> for store::Transaction {
    fn from(tx: &models::L1HandlerTransaction) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::L1HandlerTransaction(store::L1HandlerTransaction { meta })
    }
}

impl From<&models::DeployTransaction> for store::Transaction {
    fn from(tx: &models::DeployTransaction) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::DeployTransaction(store::DeployTransaction { meta })
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
        };

        store::Transaction::DeclareTransactionV0(store::DeclareTransactionV0 { meta })
    }
}

impl From<&models::DeclareTransactionV1> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV1) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::DeclareTransactionV1(store::DeclareTransactionV1 { meta })
    }
}

impl From<&models::DeclareTransactionV2> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV2) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::DeclareTransactionV2(store::DeclareTransactionV2 { meta })
    }
}

impl From<&models::DeclareTransactionV3> for store::Transaction {
    fn from(tx: &models::DeclareTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::DeclareTransactionV3(store::DeclareTransactionV3 { meta })
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
        };

        store::Transaction::DeployAccountV1(store::DeployAccountTransactionV1 { meta })
    }
}

impl From<&models::DeployAccountTransactionV3> for store::Transaction {
    fn from(tx: &models::DeployAccountTransactionV3) -> Self {
        let meta = store::TransactionMeta {
            transaction_index: u32::MAX,
            transaction_hash: tx.transaction_hash.into(),
        };

        store::Transaction::DeployAccountV3(store::DeployAccountTransactionV3 { meta })
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
        };

        store::TransactionReceipt::Invoke(store::InvokeTransactionReceipt { meta })
    }
}

impl From<&models::L1HandlerTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::L1HandlerTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
        };

        store::TransactionReceipt::L1Handler(store::L1HandlerTransactionReceipt { meta })
    }
}

impl From<&models::DeployTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeployTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
        };

        store::TransactionReceipt::Deploy(store::DeployTransactionReceipt { meta })
    }
}

impl From<&models::DeclareTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeclareTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
        };

        store::TransactionReceipt::Declare(store::DeclareTransactionReceipt { meta })
    }
}

impl From<&models::DeployAccountTransactionReceipt> for store::TransactionReceipt {
    fn from(receipt: &models::DeployAccountTransactionReceipt) -> Self {
        let meta = store::TransactionReceiptMeta {
            transaction_index: u32::MAX,
            transaction_hash: receipt.transaction_hash.into(),
        };

        store::TransactionReceipt::DeployAccount(store::DeployAccountTransactionReceipt { meta })
    }
}

impl From<&models::Event> for store::Event {
    fn from(event: &models::Event) -> Self {
        store::Event {
            from_address: event.from_address.into(),
            keys: event.keys.iter().map(|k| k.into()).collect(),
            data: event.data.iter().map(|d| d.into()).collect(),

            event_index: u32::MAX,
            transaction_index: u32::MAX,
            transaction_hash: store::FieldElement::default(),
        }
    }
}

impl From<models::BlockWithReceipts> for store::SingleBlock {
    fn from(block: models::BlockWithReceipts) -> Self {
        let header = store::BlockHeader::from(&block);
        let transactions_len = block.transactions.len();

        let mut transactions = Vec::with_capacity(transactions_len);
        let mut receipts = Vec::with_capacity(transactions_len);
        let mut events = Vec::with_capacity(transactions_len);

        let mut event_index = 0;
        for (tx_index, tx_with_rx) in block.transactions.iter().enumerate() {
            // Store tx hash for event.
            let tx_hash: store::FieldElement = tx_with_rx.transaction.transaction_hash().into();

            let mut tx = store::Transaction::from(&tx_with_rx.transaction);
            set_transaction_index(&mut tx, tx_index as u32);
            transactions.push(tx);

            for event in tx_with_rx.receipt.events() {
                let mut event: store::Event = event.into();
                event.event_index = event_index;
                event.transaction_hash = tx_hash.clone();

                events.push(event);

                event_index += 1;
            }

            let mut receipt = store::TransactionReceipt::from(&tx_with_rx.receipt);
            set_receipt_transaction_index(&mut receipt, tx_index as u32);
            receipts.push(receipt);
        }

        store::SingleBlock {
            header,
            transactions,
            receipts,
            events,
        }
    }
}

fn set_transaction_index(transaction: &mut store::Transaction, index: u32) {
    use store::Transaction::*;
    match transaction {
        InvokeTransactionV0(tx) => tx.meta.transaction_index = index,
        InvokeTransactionV1(tx) => tx.meta.transaction_index = index,
        InvokeTransactionV3(tx) => tx.meta.transaction_index = index,
        L1HandlerTransaction(tx) => tx.meta.transaction_index = index,
        DeployTransaction(tx) => tx.meta.transaction_index = index,
        DeclareTransactionV0(tx) => tx.meta.transaction_index = index,
        DeclareTransactionV1(tx) => tx.meta.transaction_index = index,
        DeclareTransactionV2(tx) => tx.meta.transaction_index = index,
        DeclareTransactionV3(tx) => tx.meta.transaction_index = index,
        DeployAccountV1(tx) => tx.meta.transaction_index = index,
        DeployAccountV3(tx) => tx.meta.transaction_index = index,
    }
}

fn set_receipt_transaction_index(receipt: &mut store::TransactionReceipt, index: u32) {
    use store::TransactionReceipt::*;
    match receipt {
        Invoke(rx) => rx.meta.transaction_index = index,
        L1Handler(rx) => rx.meta.transaction_index = index,
        Deploy(rx) => rx.meta.transaction_index = index,
        Declare(rx) => rx.meta.transaction_index = index,
        DeployAccount(rx) => rx.meta.transaction_index = index,
    }
}
