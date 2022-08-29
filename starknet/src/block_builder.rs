//! Assemble blocks data

use std::{fmt, sync::Arc, time::Duration};

use backoff::ExponentialBackoffBuilder;
use starknet::{
    core::types as sn_types,
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::{
    transaction, Block, DeclareTransaction, DeployTransaction, InvokeTransaction, Transaction,
    TransactionCommon,
};

pub struct BlockBuilder {
    pub client: Arc<SequencerGatewayProvider>,
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
    pub fn new(client: Arc<SequencerGatewayProvider>) -> Self {
        BlockBuilder { client }
    }

    #[tracing::instrument(skip(self))]
    pub async fn latest_block(&self) -> Result<Block> {
        let block = self.client.get_block(sn_types::BlockId::Latest).await?;
        block.try_into()
    }

    #[tracing::instrument(skip(self))]
    pub async fn block_by_number(&self, block_number: u64) -> Result<Block> {
        let block = self
            .client
            .get_block(sn_types::BlockId::Number(block_number))
            .await?;
        block.try_into()
    }

    pub async fn block_by_number_with_backoff(
        &self,
        block_number: u64,
        ct: CancellationToken,
    ) -> Result<Block> {
        // Exponential backoff with parameters tuned for the current sequencer
        let exp_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(10))
            .with_multiplier(2.0)
            .with_max_elapsed_time(Some(Duration::from_secs(60 * 5)))
            .build();

        backoff::future::retry(exp_backoff, || async {
            match self.block_by_number(block_number).await {
                Ok(block) => Ok(block),
                Err(BlockBuilderError::Rpc(err)) => match err {
                    SequencerGatewayProviderError::Deserialization { .. } => {
                        if ct.is_cancelled() {
                            return Err(backoff::Error::permanent(BlockBuilderError::Rpc(err)));
                        }
                        // deserialization errors are actually caused by rate limiting
                        Err(backoff::Error::transient(BlockBuilderError::Rpc(err)))
                    }
                    _ => Err(backoff::Error::permanent(BlockBuilderError::Rpc(err))),
                },
                Err(err) => Err(backoff::Error::permanent(err)),
            }
        })
        .await
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
        // some blocks have no sequencer address
        let sequencer_address = block
            .sequencer_address
            .map(|f| f.to_bytes_be().to_vec())
            .unwrap_or_default();
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
        // some blocks don't specify version
        let starknet_version = block.starknet_version.clone().unwrap_or_default();

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
