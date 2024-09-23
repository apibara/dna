use apibara_dna_common::{store::Fragment, Hash};
use rkyv::{Archive, Deserialize, Serialize};

impl Fragment for BlockHeader {
    fn tag() -> u8 {
        1
    }

    fn name() -> &'static str {
        "header"
    }
}

impl Fragment for BlockTransactions {
    fn tag() -> u8 {
        2
    }

    fn name() -> &'static str {
        "transaction"
    }
}

impl Fragment for BlockReceipts {
    fn tag() -> u8 {
        3
    }

    fn name() -> &'static str {
        "receipt"
    }
}

impl Fragment for BlockEvents {
    fn tag() -> u8 {
        4
    }

    fn name() -> &'static str {
        "event"
    }
}

impl Fragment for BlockMessages {
    fn tag() -> u8 {
        5
    }

    fn name() -> &'static str {
        "message"
    }
}

/// Transactions in a block.
#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BlockTransactions(pub Vec<Transaction>);

/// Transactions receipts in a block.
#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BlockReceipts(pub Vec<TransactionReceipt>);

/// Events in a block.
#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BlockEvents(pub Vec<Event>);

/// Messages in a block.
#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct BlockMessages(pub Vec<MessageToL1>);

/// A field element is encoded as a 32-byte array.
#[derive(Archive, Serialize, Deserialize, PartialEq, Clone, Copy, Default, PartialOrd, Eq, Ord)]
pub struct FieldElement(pub [u8; 32]);

/// The block header.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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
pub struct ResourcePrice {
    pub price_in_fri: FieldElement,
    pub price_in_wei: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub enum L1DataAvailabilityMode {
    Blob,
    Calldata,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct FeePayment {
    pub amount: FieldElement,
    pub unit: PriceUnit,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub enum PriceUnit {
    Wei,
    Fri,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecutionResources {
    pub computation: ComputationResources,
    pub data_availability: DataResources,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub enum ExecutionResult {
    Succeeded,
    Reverted { reason: String },
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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
pub struct DataResources {
    pub l1_gas: u64,
    pub l1_data_gas: u64,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct Event {
    pub address: FieldElement,
    pub keys: Vec<FieldElement>,
    pub data: Vec<FieldElement>,

    pub event_index: u32,
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
    pub transaction_reverted: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct MessageToL1 {
    pub from_address: FieldElement,
    pub to_address: FieldElement,
    pub payload: Vec<FieldElement>,

    pub message_index: u32,
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
    pub transaction_reverted: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd, Ord, Hash, Eq)]
pub enum TransactionType {
    InvokeTransactionV0,
    InvokeTransactionV1,
    InvokeTransactionV3,

    L1HandlerTransaction,
    DeployTransaction,

    DeclareTransactionV0,
    DeclareTransactionV1,
    DeclareTransactionV2,
    DeclareTransactionV3,

    DeployAccountTransactionV1,
    DeployAccountTransactionV3,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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

    DeployAccountTransactionV1(DeployAccountTransactionV1),
    DeployAccountTransactionV3(DeployAccountTransactionV3),
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct TransactionMeta {
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
    pub transaction_reverted: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct InvokeTransactionV0 {
    pub meta: TransactionMeta,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub contract_address: FieldElement,
    pub entry_point_selector: FieldElement,
    pub calldata: Vec<FieldElement>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct InvokeTransactionV1 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub calldata: Vec<FieldElement>,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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
pub struct L1HandlerTransaction {
    pub meta: TransactionMeta,
    pub nonce: u64,
    pub contract_address: FieldElement,
    pub entry_point_selector: FieldElement,
    pub calldata: Vec<FieldElement>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeployTransaction {
    pub meta: TransactionMeta,
    pub contract_address_salt: FieldElement,
    pub constructor_calldata: Vec<FieldElement>,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeclareTransactionV0 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeclareTransactionV1 {
    pub meta: TransactionMeta,
    pub sender_address: FieldElement,
    pub max_fee: FieldElement,
    pub signature: Vec<FieldElement>,
    pub nonce: FieldElement,
    pub class_hash: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
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
pub enum TransactionReceipt {
    Invoke(InvokeTransactionReceipt),
    L1Handler(L1HandlerTransactionReceipt),
    Declare(DeclareTransactionReceipt),
    Deploy(DeployTransactionReceipt),
    DeployAccount(DeployAccountTransactionReceipt),
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct TransactionReceiptMeta {
    pub transaction_index: u32,
    pub transaction_hash: FieldElement,
    pub actual_fee: FeePayment,
    pub execution_resources: ExecutionResources,
    pub execution_result: ExecutionResult,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct InvokeTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct L1HandlerTransactionReceipt {
    pub meta: TransactionReceiptMeta,
    pub message_hash: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeclareTransactionReceipt {
    pub meta: TransactionReceiptMeta,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeployTransactionReceipt {
    pub meta: TransactionReceiptMeta,
    pub contract_address: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeployAccountTransactionReceipt {
    pub meta: TransactionReceiptMeta,
    pub contract_address: FieldElement,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct ResourceBoundsMapping {
    pub l1_gas: ResourceBounds,
    pub l2_gas: ResourceBounds,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub struct ResourceBounds {
    pub max_amount: u64,
    pub max_price_per_unit: u128,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
pub enum DataAvailabilityMode {
    L1,
    L2,
}

impl From<FieldElement> for Hash {
    fn from(value: FieldElement) -> Self {
        Hash(value.0.to_vec())
    }
}

impl std::fmt::Debug for FieldElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "F(0x{})", hex::encode(self.0))
    }
}

impl Transaction {
    pub fn transaction_index(&self) -> u32 {
        match self {
            Transaction::InvokeTransactionV0(tx) => tx.meta.transaction_index,
            Transaction::InvokeTransactionV1(tx) => tx.meta.transaction_index,
            Transaction::InvokeTransactionV3(tx) => tx.meta.transaction_index,

            Transaction::L1HandlerTransaction(tx) => tx.meta.transaction_index,
            Transaction::DeployTransaction(tx) => tx.meta.transaction_index,

            Transaction::DeclareTransactionV0(tx) => tx.meta.transaction_index,
            Transaction::DeclareTransactionV1(tx) => tx.meta.transaction_index,
            Transaction::DeclareTransactionV2(tx) => tx.meta.transaction_index,
            Transaction::DeclareTransactionV3(tx) => tx.meta.transaction_index,

            Transaction::DeployAccountTransactionV1(tx) => tx.meta.transaction_index,
            Transaction::DeployAccountTransactionV3(tx) => tx.meta.transaction_index,
        }
    }
}
