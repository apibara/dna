use std::collections::BTreeMap;

use rkyv::{with::AsVec, Archive, Deserialize, Serialize};

/// A field element is encoded as a 32-byte array.
#[derive(
    Archive, Serialize, Deserialize, Debug, PartialEq, Clone, Copy, Default, PartialOrd, Eq, Ord,
)]
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
pub struct FeePayment {
    pub amount: FieldElement,
    pub unit: PriceUnit,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum PriceUnit {
    Wei,
    Fri,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ExecutionResources {
    pub computation: ComputationResources,
    pub data_availability: DataResources,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum ExecutionResult {
    Succeeded,
    Reverted { reason: String },
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ComputationResources {
    pub steps: u64,
    pub memory_holes: Option<u64>,
    pub range_check_builtin_applications: Option<u64>,
    pub pedersen_builtin_applications: Option<u64>,
    pub poseidon_builtin_applications: Option<u64>,
    pub ec_op_builtin_applications: Option<u64>,
    pub ecdsa_builtin_applications: Option<u64>,
    pub bitwise_builtin_applications: Option<u64>,
    pub keccak_builtin_applications: Option<u64>,
    pub segment_arena_builtin: Option<u64>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DataResources {
    pub l1_gas: u64,
    pub l1_data_gas: u64,
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
pub struct MessageToL1 {
    pub from_address: FieldElement,
    pub to_address: FieldElement,
    pub payload: Vec<FieldElement>,

    pub message_index: u32,
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
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub contract_address: FieldElement,
    pub entry_point_selector: FieldElement,
    pub calldata: Vec<FieldElement>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionV1 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub calldata: Vec<FieldElement>,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvokeTransactionV3 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub calldata: Vec<FieldElement>,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub resource_bounds: ResourceBoundsMapping,
    pub tip: u64,
    pub paymaster_data: Vec<FieldElement>,
    pub account_deployment_data: Vec<FieldElement>,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct L1HandlerTransaction {
    pub meta: TransactionMeta,
    pub nonce: u64,
    pub contract_address: FieldElement,
    pub entry_point_selector: FieldElement,
    pub calldata: Vec<FieldElement>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployTransaction {
    pub meta: TransactionMeta,
    pub contract_address_salt: FieldElement,
    pub constructor_calldata: Vec<FieldElement>,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV0 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV1 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV2 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub compiled_class_hash: FieldElement,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeclareTransactionV3 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub compiled_class_hash: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub class_hash: FieldElement,
    pub resource_bounds: ResourceBoundsMapping,
    pub tip: u64,
    pub paymaster_data: Vec<FieldElement>,
    pub account_deployment_data: Vec<FieldElement>,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionV1 {
    pub meta: TransactionMeta,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub contract_address_salt: FieldElement,
    pub constructor_calldata: Vec<FieldElement>,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionV3 {
    pub meta: TransactionMeta,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub contract_address_salt: FieldElement,
    pub constructor_calldata: Vec<FieldElement>,
    pub class_hash: FieldElement,
    pub resource_bounds: ResourceBoundsMapping,
    pub tip: u64,
    pub paymaster_data: Vec<FieldElement>,
    pub nonce_data_availability_mode: DataAvailabilityMode,
    pub fee_data_availability_mode: DataAvailabilityMode,
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
    pub actual_fee: FeePayment,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
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
    pub message_hash: Vec<u8>,
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
    pub contract_address: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeployAccountTransactionReceipt {
    pub meta: TransactionReceiptMeta,
    pub contract_address: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ResourceBoundsMapping {
    pub l1_gas: ResourceBounds,
    pub l2_gas: ResourceBounds,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct ResourceBounds {
    pub max_amount: u64,
    pub max_price_per_unit: u128,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum DataAvailabilityMode {
    L1,
    L2,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SingleBlock {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    pub receipts: Vec<TransactionReceipt>,
    pub events: Vec<Event>,
    pub messages: Vec<MessageToL1>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Index {
    #[with(AsVec)]
    pub event_by_address: BTreeMap<FieldElement, Bitmap>,
    #[with(AsVec)]
    pub event_by_key_0: BTreeMap<FieldElement, Bitmap>,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct Bitmap(pub Vec<u8>);

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct SegmentGroup {
    pub index: Index,
}

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
pub type TransactionSegment = Segment<BlockData<Transaction>>;
pub type TransactionReceiptSegment = Segment<BlockData<TransactionReceipt>>;
pub type EventSegment = Segment<BlockData<Event>>;
pub type MessageSegment = Segment<BlockData<MessageToL1>>;

impl<T> Default for Segment<T> {
    fn default() -> Self {
        Self { blocks: Vec::new() }
    }
}

impl<T> Segment<T> {
    pub fn reset(&mut self) {
        self.blocks.clear();
    }
}

impl TransactionReceipt {
    pub fn meta(&self) -> &TransactionReceiptMeta {
        match self {
            TransactionReceipt::Invoke(receipt) => &receipt.meta,
            TransactionReceipt::L1Handler(receipt) => &receipt.meta,
            TransactionReceipt::Declare(receipt) => &receipt.meta,
            TransactionReceipt::Deploy(receipt) => &receipt.meta,
            TransactionReceipt::DeployAccount(receipt) => &receipt.meta,
        }
    }
}
