//! Assemble blocks data

use std::{fmt, sync::Arc, time::Duration};

use backoff::{exponential::ExponentialBackoff, ExponentialBackoffBuilder, SystemClock};
use starknet::{
    core::types::{self as sn_types, FieldElement},
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio_util::sync::CancellationToken;

use crate::core::{
    transaction, Block, BlockHash, DeclareTransaction, DeployTransaction, InvokeTransaction,
    L1HandlerTransaction, Transaction, TransactionCommon,
};

pub struct BlockBuilder {
    pub client: Arc<SequencerGatewayProvider>,
    pub exponential_backoff: ExponentialBackoff<SystemClock>,
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
        let exponential_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(10))
            .with_multiplier(2.0)
            .with_max_elapsed_time(Some(Duration::from_secs(60 * 5)))
            .build();
        BlockBuilder {
            client,
            exponential_backoff,
        }
    }

    #[tracing::instrument(level = "debug", skip(self, ct))]
    pub async fn latest_block_with_backoff(&self, ct: CancellationToken) -> Result<Block> {
        let fetch = || self.fetch_block(sn_types::BlockId::Latest, &ct);
        backoff::future::retry(self.exponential_backoff.clone(), fetch).await
    }

    #[tracing::instrument(level = "debug", skip(self, ct))]
    pub async fn block_by_number_with_backoff(
        &self,
        block_number: u64,
        ct: CancellationToken,
    ) -> Result<Block> {
        let fetch = || self.fetch_block(sn_types::BlockId::Number(block_number), &ct);
        backoff::future::retry(self.exponential_backoff.clone(), fetch).await
    }

    async fn fetch_block(
        &self,
        block_id: sn_types::BlockId,
        ct: &CancellationToken,
    ) -> std::result::Result<Block, backoff::Error<BlockBuilderError>> {
        match self.client.get_block(block_id).await {
            Ok(block) => {
                let block = block.try_into().map_err(backoff::Error::permanent)?;
                Ok(block)
            }
            Err(err @ SequencerGatewayProviderError::Deserialization { .. }) => {
                if ct.is_cancelled() {
                    return Err(backoff::Error::permanent(BlockBuilderError::Rpc(err)));
                }
                // deserialization errors are actually caused by rate limiting
                Err(backoff::Error::transient(BlockBuilderError::Rpc(err)))
            }
            Err(err) => Err(backoff::Error::permanent(BlockBuilderError::Rpc(err))),
        }
    }
}

impl TryFrom<sn_types::Block> for Block {
    type Error = BlockBuilderError;

    fn try_from(block: sn_types::Block) -> std::result::Result<Self, Self::Error> {
        let block_hash = block
            .block_hash
            .ok_or(BlockBuilderError::UnexpectedPendingBlock)?
            .into();
        let parent_block_hash = block.parent_block_hash.into();
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
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
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

impl From<FieldElement> for BlockHash {
    fn from(value: FieldElement) -> Self {
        let hash = value.to_bytes_be().to_vec();
        BlockHash { hash }
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
            sn_types::TransactionType::L1Handler(l1_handler) => {
                let l1_handler = l1_handler.try_into()?;
                transaction::Transaction::L1Handler(l1_handler)
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
        let contract_address = tx.contract_address.to_bytes_be().to_vec();
        let contract_address_salt = tx.contract_address_salt.to_bytes_be().to_vec();
        let constructor_calldata = tx
            .constructor_calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let common = TransactionCommon {
            hash,
            max_fee: Vec::new(),
            signature: Vec::new(),
            nonce: Vec::new(),
        };
        Ok(DeployTransaction {
            common: Some(common),
            constructor_calldata,
            contract_address,
            contract_address_salt,
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

impl TryFrom<&sn_types::L1HandlerTransaction> for L1HandlerTransaction {
    type Error = BlockBuilderError;

    fn try_from(tx: &sn_types::L1HandlerTransaction) -> std::result::Result<Self, Self::Error> {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let common = TransactionCommon {
            hash,
            max_fee: Vec::default(),
            signature: Vec::default(),
            nonce: Vec::new(),
        };

        let contract_address = tx.contract_address.to_bytes_be().to_vec();
        let entry_point_selector = tx.entry_point_selector.to_bytes_be().to_vec();
        let calldata = tx
            .calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        Ok(L1HandlerTransaction {
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
