use apibara_dna_common::{Cursor, Hash};
pub use starknet::core::types::{
    BlockWithReceipts, ComputationResources, ContractStorageDiffItem, DataAvailabilityMode,
    DataResources, DeclareTransaction, DeclareTransactionReceipt, DeclareTransactionV0,
    DeclareTransactionV1, DeclareTransactionV2, DeclareTransactionV3, DeclaredClassItem,
    DeployAccountTransaction, DeployAccountTransactionReceipt, DeployAccountTransactionV1,
    DeployAccountTransactionV3, DeployTransaction, DeployTransactionReceipt, DeployedContractItem,
    Event, ExecutionResources, ExecutionResult, FeePayment, Felt as FieldElement,
    InvokeTransaction, InvokeTransactionReceipt, InvokeTransactionV0, InvokeTransactionV1,
    InvokeTransactionV3, L1DataAvailabilityMode, L1HandlerTransaction, L1HandlerTransactionReceipt,
    MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes, MaybePendingStateUpdate, MsgToL1,
    NonceUpdate, PriceUnit, ReplacedClassItem, ResourceBounds, ResourceBoundsMapping,
    ResourcePrice, StateDiff, StateUpdate, StorageEntry, Transaction, TransactionReceipt,
    TransactionWithReceipt,
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
