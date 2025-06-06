use apibara_dna_common::{Cursor, Hash};
pub use starknet::core::types::{
    BlockWithReceipts, CallType, ContractStorageDiffItem, DataAvailabilityMode,
    DeclareTransactionContent, DeclareTransactionReceipt, DeclareTransactionTrace,
    DeclareTransactionV0Content, DeclareTransactionV1Content, DeclareTransactionV2Content,
    DeclareTransactionV3Content, DeclaredClassItem, DeployAccountTransactionContent,
    DeployAccountTransactionReceipt, DeployAccountTransactionTrace,
    DeployAccountTransactionV1Content, DeployAccountTransactionV3Content, DeployTransactionContent,
    DeployTransactionReceipt, DeployedContractItem, Event, ExecuteInvocation, ExecutionResources,
    ExecutionResult, FeePayment, Felt as FieldElement, FunctionCall, FunctionInvocation,
    InvokeTransactionContent, InvokeTransactionReceipt, InvokeTransactionTrace,
    InvokeTransactionV0Content, InvokeTransactionV1Content, InvokeTransactionV3Content,
    L1DataAvailabilityMode, L1HandlerTransactionContent, L1HandlerTransactionReceipt,
    L1HandlerTransactionTrace, MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes,
    MaybePendingStateUpdate, MsgToL1, NonceUpdate, PendingBlockWithReceipts, PendingStateUpdate,
    PriceUnit, ReplacedClassItem, ResourceBounds, ResourceBoundsMapping, ResourcePrice, StateDiff,
    StateUpdate, StorageEntry, TransactionContent, TransactionReceipt, TransactionTrace,
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
