use apibara_dna_common::{Cursor, Hash};
pub use starknet::core::types::{
    BlockWithReceipts, CallType, ComputationResources, ContractStorageDiffItem,
    DataAvailabilityMode, DataResources, DeclareTransaction, DeclareTransactionReceipt,
    DeclareTransactionTrace, DeclareTransactionV0, DeclareTransactionV1, DeclareTransactionV2,
    DeclareTransactionV3, DeclaredClassItem, DeployAccountTransaction,
    DeployAccountTransactionReceipt, DeployAccountTransactionTrace, DeployAccountTransactionV1,
    DeployAccountTransactionV3, DeployTransaction, DeployTransactionReceipt, DeployedContractItem,
    Event, ExecuteInvocation, ExecutionResources, ExecutionResult, FeePayment,
    Felt as FieldElement, FunctionCall, FunctionInvocation, InvokeTransaction,
    InvokeTransactionReceipt, InvokeTransactionTrace, InvokeTransactionV0, InvokeTransactionV1,
    InvokeTransactionV3, L1DataAvailabilityMode, L1HandlerTransaction, L1HandlerTransactionReceipt,
    L1HandlerTransactionTrace, MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes,
    MaybePendingStateUpdate, MsgToL1, NonceUpdate, PendingBlockWithReceipts, PendingStateUpdate,
    PriceUnit, ReplacedClassItem, ResourceBounds, ResourceBoundsMapping, ResourcePrice, StateDiff,
    StateUpdate, StorageEntry, Transaction, TransactionReceipt, TransactionTrace,
    TransactionTraceWithHash, TransactionWithReceipt,
};

pub trait BlockExt {
    fn is_finalized(&self) -> bool;
    fn cursor(&self) -> Option<Cursor>;
}

impl BlockExt for MaybePendingBlockWithTxHashes {
    fn is_finalized(&self) -> bool {
        let MaybePendingBlockWithTxHashes::Block(block) = self else {
            return false;
        };

        block.status == starknet::core::types::BlockStatus::AcceptedOnL1
    }

    fn cursor(&self) -> Option<Cursor> {
        let MaybePendingBlockWithTxHashes::Block(block) = self else {
            return None;
        };

        let number = block.block_number;
        let hash = block.block_hash.to_bytes_be().to_vec();

        Cursor::new(number, Hash(hash)).into()
    }
}
