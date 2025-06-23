use std::sync::Arc;

use alloy::{
    consensus::{EthereumTxEnvelope, Header, Transaction, TxEip4844Variant},
    eips::Typed2718,
    primitives::Address,
};
use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema};

use crate::builder::{
    AddressBuilder, B256Builder, BinaryBuilder, U8Builder, U32Builder, U64Builder, U256Builder,
};

#[derive(Default)]
pub struct TransactionBatchBuilder {
    pub count: usize,
    pub block_number: U32Builder,
    pub block_hash: B256Builder,
    pub transaction_index: U32Builder,
    pub transaction_hash: B256Builder,
    pub nonce: U32Builder,
    pub from_address: AddressBuilder,
    pub to_address: AddressBuilder,
    pub value: U256Builder,
    pub input: BinaryBuilder,
    pub gas_limit: U64Builder,
    // pub gas_used: U64Builder,
    pub gas_price: U64Builder,
    pub transaction_type: U8Builder,
    pub max_priority_fee_per_gas: U64Builder,
    pub max_fee_per_gas: U64Builder,
}

impl TransactionBatchBuilder {
    pub fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("block_number", U32Builder::data_type(), false),
            Field::new("block_hash", B256Builder::data_type(), false),
            Field::new("transaction_index", U32Builder::data_type(), false),
            Field::new("transaction_hash", B256Builder::data_type(), false),
            Field::new("nonce", U32Builder::data_type(), false),
            Field::new("from_address", AddressBuilder::data_type(), false),
            Field::new("to_address", AddressBuilder::data_type(), true),
            Field::new("value", U256Builder::data_type(), false),
            Field::new("input", BinaryBuilder::data_type(), false),
            Field::new("gas_limit", U64Builder::data_type(), false),
            // Field::new("gas_used", U64Builder::data_type(), false),
            Field::new("gas_price", U64Builder::data_type(), true),
            Field::new("transaction_type", U8Builder::data_type(), false),
            Field::new("max_priority_fee_per_gas", U64Builder::data_type(), true),
            Field::new("max_fee_per_gas", U64Builder::data_type(), false),
        ]))
    }

    pub fn append_transaction(
        &mut self,
        header: &Header,
        tx_index: usize,
        tx: &EthereumTxEnvelope<TxEip4844Variant>,
        signer: &Address,
    ) {
        self.block_number.append_value(header.number as _);
        self.block_hash.append_value(header.hash_slow().as_ref());
        self.transaction_index.append_value(tx_index as _);
        self.transaction_hash.append_value(tx.hash().as_ref());
        self.nonce.append_value(tx.nonce() as _);
        self.from_address.append_value(signer.as_ref());
        if let Some(ref to) = tx.to() {
            self.to_address.append_value(to.as_ref());
        } else {
            self.to_address.append_null();
        }
        self.value.append_value(&tx.value().to_be_bytes());
        self.input.append_value(tx.input().as_ref());
        self.gas_limit.append_value(tx.gas_limit() as _);
        if let Some(gas_price) = tx.gas_price() {
            self.gas_price.append_value(gas_price as _);
        } else {
            self.gas_price.append_null();
        }
        self.transaction_type.append_value(tx.ty());
        if let Some(max_fee_per_gas) = tx.max_priority_fee_per_gas() {
            self.max_priority_fee_per_gas
                .append_value(max_fee_per_gas as _);
        } else {
            self.max_priority_fee_per_gas.append_null();
        }
        self.max_fee_per_gas.append_value(tx.max_fee_per_gas() as _);
    }

    pub fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema(),
            vec![
                self.block_number.finish(),
                self.block_hash.finish(),
                self.transaction_index.finish(),
                self.transaction_hash.finish(),
                self.nonce.finish(),
                self.from_address.finish(),
                self.to_address.finish(),
                self.value.finish(),
                self.input.finish(),
                self.gas_limit.finish(),
                self.gas_price.finish(),
                self.transaction_type.finish(),
                self.max_priority_fee_per_gas.finish(),
                self.max_fee_per_gas.finish(),
            ],
        )
        .expect("failed to build record batch")
    }
}
