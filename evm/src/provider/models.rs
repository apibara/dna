pub use alloy_consensus::{
    EthereumTxEnvelope, Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant,
    TxEip4844WithSidecar, TxEip7702, TxEnvelope, TxLegacy,
};
pub use alloy_primitives::{Address, Bloom, ChainId, PrimitiveSignature, TxKind, B256, U128, U256};
pub use alloy_rpc_types::{
    AccessList, AccessListItem, Block, Header, Log, Transaction, TransactionReceipt, Withdrawal,
};
pub use alloy_rpc_types_trace::parity::{
    Action, CallAction, CallOutput, CallType, CreateAction, CreateOutput, RewardAction, RewardType,
    SelfdestructAction, TraceOutput, TraceResults, TraceResultsWithTransactionHash,
    TransactionTrace,
};
use apibara_dna_common::{chain::BlockInfo, Cursor, Hash};

pub type BlockWithTxHashes = Block<B256>;

pub trait BlockExt {
    fn cursor(&self) -> Cursor;
    fn block_info(&self) -> BlockInfo;
}

impl<T> BlockExt for Block<T> {
    fn cursor(&self) -> Cursor {
        let number = self.header.number;
        let hash = self.header.hash;
        Cursor::new(number, Hash(hash.to_vec()))
    }

    fn block_info(&self) -> BlockInfo {
        let number = self.header.number;
        let hash = self.header.hash;
        let parent = self.header.parent_hash;

        BlockInfo {
            number,
            hash: Hash(hash.to_vec()),
            parent: Hash(parent.to_vec()),
        }
    }
}
