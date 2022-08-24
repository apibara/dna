//! Assemble blocks data

use std::{fmt, sync::Arc};

use starknet::{
    core::types as sn_types,
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tracing::info;

use crate::core::{
    transaction, Block, DeclareTransaction, DeployTransaction, InvokeTransaction, Transaction,
    TransactionCommon,
};

pub struct BlockBuilder {
    pub client: SequencerGatewayProvider,
}

#[derive(Debug, thiserror::Error)]
pub enum BlockBuilderError {
    #[error("error performing a starknet gateway request")]
    Rpc(#[from] SequencerGatewayProviderError),
    #[error("unexpected pending block")]
    UnexpectedPendingBlock,
}

pub type Result<T> = std::result::Result<T, BlockBuilderError>;

impl BlockBuilder {
    /// Creates a new [BlockBuilder] with the given StarkNet JSON-RPC client.
    pub fn new() -> Self {
        let client = SequencerGatewayProvider::starknet_alpha_goerli();
        BlockBuilder { client }
    }

    #[tracing::instrument]
    pub async fn latest_block(&self) -> Result<Block> {
        let block = self.client.get_block(sn_types::BlockId::Latest).await?;
        Ok(block.try_into()?)
    }
}

impl TryFrom<sn_types::Block> for Block {
    type Error = BlockBuilderError;

    fn try_from(block: sn_types::Block) -> std::result::Result<Self, Self::Error> {
        let block_hash = block
            .block_hash
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?
            .to_bytes_be()
            .to_vec();
        let parent_block_hash = block.parent_block_hash.to_bytes_be().to_vec();
        let block_number = block
            .block_number
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?;
        let sequencer_address = block
            .sequencer_address
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?
            .to_bytes_be()
            .to_vec();
        let state_root = block
            .state_root
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?
            .to_bytes_be()
            .to_vec();
        let gas_price = block.gas_price.to_bytes_be().to_vec();
        let timestamp = prost_types::Timestamp {
            nanos: 0,
            seconds: block.timestamp as i64,
        };
        let starknet_version = block
            .starknet_version
            .clone()
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?;

        let transactions = block
            .transactions
            .iter()
            .map(|tx| tx.try_into())
            .collect::<Result<Vec<_>>>()?;

        Ok(Block {
            block_hash,
            parent_block_hash,
            block_number,
            sequencer_address,
            state_root,
            gas_price,
            timestamp: Some(timestamp),
            starknet_version,
            transactions,
        })
    }
}

impl TryFrom<&sn_types::TransactionType> for Transaction {
    type Error = BlockBuilderError;

    fn try_from(tx: &sn_types::TransactionType) -> std::result::Result<Self, Self::Error> {
        let inner = match tx {
            sn_types::TransactionType::Deploy(deploy) => {
                let deploy = deploy.try_into()?;
                transaction::Transaction::Deploy(deploy)
            }
            sn_types::TransactionType::Declare(declare) => {
                let declare = declare.try_into()?;
                transaction::Transaction::Declare(declare)
            }
            sn_types::TransactionType::InvokeFunction(invoke) => {
                let invoke = invoke.try_into()?;
                transaction::Transaction::Invoke(invoke)
            }
        };
        Ok(Transaction {
            transaction: Some(inner),
        })
    }
}

impl TryFrom<&sn_types::DeployTransaction> for DeployTransaction {
    type Error = BlockBuilderError;

    fn try_from(tx: &sn_types::DeployTransaction) -> std::result::Result<Self, Self::Error> {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let common = TransactionCommon {
            hash,
            max_fee: Vec::new(),
            signature: Vec::new(),
            nonce: Vec::new(),
        };
        Ok(DeployTransaction {
            common: Some(common),
        })
    }
}

impl TryFrom<&sn_types::DeclareTransaction> for DeclareTransaction {
    type Error = BlockBuilderError;

    fn try_from(tx: &sn_types::DeclareTransaction) -> std::result::Result<Self, Self::Error> {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let max_fee = tx.max_fee.to_bytes_be().to_vec();
        let signature = tx
            .signature
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let nonce = tx.nonce.to_bytes_be().to_vec();
        let common = TransactionCommon {
            hash,
            max_fee,
            signature,
            nonce,
        };

        let class_hash = tx.class_hash.to_bytes_be().to_vec();
        let sender_address = tx.sender_address.to_bytes_be().to_vec();

        Ok(DeclareTransaction {
            common: Some(common),
            class_hash,
            sender_address,
        })
    }
}

impl TryFrom<&sn_types::InvokeFunctionTransaction> for InvokeTransaction {
    type Error = BlockBuilderError;

    fn try_from(
        tx: &sn_types::InvokeFunctionTransaction,
    ) -> std::result::Result<Self, Self::Error> {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let max_fee = tx.max_fee.to_bytes_be().to_vec();
        let signature = tx
            .signature
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let common = TransactionCommon {
            hash,
            max_fee,
            signature,
            nonce: Vec::new(),
        };

        let contract_address = tx.contract_address.to_bytes_be().to_vec();
        let entry_point_selector = tx.entry_point_selector.to_bytes_be().to_vec();
        let calldata = tx
            .calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        Ok(InvokeTransaction {
            common: Some(common),
            contract_address,
            entry_point_selector,
            calldata,
        })
    }
}

impl fmt::Debug for BlockBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockBuilder")
    }
}
