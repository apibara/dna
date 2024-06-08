use std::collections::BTreeMap;

use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

/// A field element is encoded as a 32-byte array.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
#[archive(compare(PartialEq), check_bytes)]
pub struct FieldElement(pub [u8; 32]);

/// The block header.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct BlockHeader {
    pub block_hash: FieldElement,
    pub parent_block_hash: FieldElement,
    pub block_number: u64,
    pub sequencer_address: FieldElement,
    pub new_root: FieldElement,
    pub timestamp: u64,
    pub starknet_version: String,
    pub l1_gas_price: ResourcePrice,
    pub l1_data_gas_price: ResourcePrice,
    pub l1_data_availability_mode: L1DataAvailabilityMode,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ResourcePrice {
    pub price_in_fri: FieldElement,
    pub price_in_wei: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum L1DataAvailabilityMode {
    Blob,
    Calldata,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct Event {
    pub from_address: FieldElement,
    pub keys: Vec<FieldElement>,
    pub data: Vec<FieldElement>,

    pub event_index: u32,
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum Transaction {
    InvokeTransactionV0(InvokeTransactionV0),
    InvokeTransactionV1(InvokeTransactionV1),
    InvokeTransactionV3(InvokeTransactionV3),

    L1HandlerTransaction(L1HandlerTransaction),
    DeployTransaction(DeployTransaction),

    DeclareTransactionV0(DeclareTransactionV0),
    DeclareTransactionV1(DeclareTransactionV1),
    DeclareTransactionV2(DeclareTransactionV2),
    DeclareTransactionV3(DeclareTransactionV3),

    DeployAccountV1(DeployAccountTransactionV1),
    DeployAccountV3(DeployAccountTransactionV3),
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct TransactionMeta {
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionV0 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionV1 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionV3 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct L1HandlerTransaction {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployTransaction {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV0 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV1 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV2 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV3 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionV1 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionV3 {
    pub meta: TransactionMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum TransactionReceipt {
    Invoke(InvokeTransactionReceipt),
    L1Handler(L1HandlerTransactionReceipt),
    Declare(DeclareTransactionReceipt),
    Deploy(DeployTransactionReceipt),
    DeployAccount(DeployAccountTransactionReceipt),
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct TransactionReceiptMeta {
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct L1HandlerTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SingleBlock {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<TransactionReceipt>,
    pub events: Vec<Event>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentGroup {
    #[with(AsVec)]
    pub event_by_address: BTreeMap<FieldElement, Bitmap>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Bitmap(Vec<u8>);

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct Segment<T> {
    pub blocks: Vec<T>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct BlockData<T> {
    pub block_number: u64,
    pub data: Vec<T>,
}

pub type BlockHeaderSegment = Segment<BlockHeader>;

pub type EventSegment = Segment<BlockData<Event>>;
pub type TransactionSegment = Segment<BlockData<Transaction>>;
pub type TransactionReceiptSegment = Segment<BlockData<TransactionReceipt>>;
