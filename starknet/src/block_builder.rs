//! Assemble blocks data

use std::{fmt, sync::Arc, time::Duration};

use backoff::{exponential::ExponentialBackoff, ExponentialBackoffBuilder, SystemClock};
use starknet::{
    core::types::{self as sn_types, FieldElement},
    providers::{Provider, SequencerGatewayProvider, SequencerGatewayProviderError},
};
use tokio_util::sync::CancellationToken;

use crate::core::{
    transaction, Block, BlockHash, BuiltinInstanceCounter, DeclareTransaction, DeployTransaction,
    Event, ExecutionResources, InvokeTransaction, L1HandlerTransaction, L1ToL2Message,
    L2ToL1Message, Transaction, TransactionCommon, TransactionReceipt,
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

        let transactions = block.transactions.iter().map(|tx| tx.into()).collect();

        let transaction_receipts = block
            .transaction_receipts
            .iter()
            .map(|rx| rx.into())
            .collect();

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
            transaction_receipts,
        })
    }
}

impl From<FieldElement> for BlockHash {
    fn from(value: FieldElement) -> Self {
        let hash = value.to_bytes_be().to_vec();
        BlockHash { hash }
    }
}

impl From<&sn_types::TransactionType> for Transaction {
    fn from(tx: &sn_types::TransactionType) -> Self {
        let inner = match tx {
            sn_types::TransactionType::Deploy(deploy) => {
                let deploy = deploy.into();
                transaction::Transaction::Deploy(deploy)
            }
            sn_types::TransactionType::Declare(declare) => {
                let declare = declare.into();
                transaction::Transaction::Declare(declare)
            }
            sn_types::TransactionType::InvokeFunction(invoke) => {
                let invoke = invoke.into();
                transaction::Transaction::Invoke(invoke)
            }
            sn_types::TransactionType::L1Handler(l1_handler) => {
                let l1_handler = l1_handler.into();
                transaction::Transaction::L1Handler(l1_handler)
            }
        };
        Transaction {
            transaction: Some(inner),
        }
    }
}

impl From<&sn_types::DeployTransaction> for DeployTransaction {
    fn from(tx: &sn_types::DeployTransaction) -> Self {
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
        DeployTransaction {
            common: Some(common),
            constructor_calldata,
            contract_address,
            contract_address_salt,
        }
    }
}

impl From<&sn_types::DeclareTransaction> for DeclareTransaction {
    fn from(tx: &sn_types::DeclareTransaction) -> Self {
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

        DeclareTransaction {
            common: Some(common),
            class_hash,
            sender_address,
        }
    }
}

impl From<&sn_types::InvokeFunctionTransaction> for InvokeTransaction {
    fn from(tx: &sn_types::InvokeFunctionTransaction) -> Self {
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
        InvokeTransaction {
            common: Some(common),
            contract_address,
            entry_point_selector,
            calldata,
        }
    }
}

impl From<&sn_types::L1HandlerTransaction> for L1HandlerTransaction {
    fn from(tx: &sn_types::L1HandlerTransaction) -> Self {
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
        L1HandlerTransaction {
            common: Some(common),
            contract_address,
            entry_point_selector,
            calldata,
        }
    }
}

impl From<&sn_types::ConfirmedTransactionReceipt> for TransactionReceipt {
    fn from(value: &sn_types::ConfirmedTransactionReceipt) -> Self {
        let transaction_hash = value.transaction_hash.to_bytes_be().to_vec();
        let actual_fee = value.actual_fee.to_bytes_be().to_vec();
        let execution_resources: Option<ExecutionResources> =
            value.execution_resources.as_ref().map(|er| er.into());

        let l1_to_l2_consumed_message = value.l1_to_l2_consumed_message.as_ref().map(|m| m.into());

        let l2_to_l1_messages = value.l2_to_l1_messages.iter().map(|m| m.into()).collect();

        let events = value.events.iter().map(|m| m.into()).collect();

        TransactionReceipt {
            transaction_hash,
            transaction_index: value.transaction_index,
            execution_resources,
            actual_fee,
            l1_to_l2_consumed_message,
            l2_to_l1_messages,
            events,
        }
    }
}

impl From<&sn_types::ExecutionResources> for ExecutionResources {
    fn from(value: &sn_types::ExecutionResources) -> Self {
        let builtin_instance_counter = (&value.builtin_instance_counter).into();

        ExecutionResources {
            n_steps: value.n_steps,
            n_memory_holes: value.n_memory_holes,
            builtin_instance_counter: Some(builtin_instance_counter),
        }
    }
}

impl From<&sn_types::BuiltinInstanceCounter> for BuiltinInstanceCounter {
    fn from(value: &sn_types::BuiltinInstanceCounter) -> Self {
        BuiltinInstanceCounter {
            pedersen_builtin: value.pedersen_builtin,
            range_check_builtin: value.range_check_builtin,
            bitwise_builtin: value.bitwise_builtin,
            output_builtin: value.output_builtin,
            ecdsa_builtin: value.ecdsa_builtin,
            ec_op_builtin: value.ec_op_builtin,
        }
    }
}

impl From<&sn_types::L1ToL2Message> for L1ToL2Message {
    fn from(value: &sn_types::L1ToL2Message) -> Self {
        let from_address = value.from_address.as_bytes().to_vec();
        let to_address = value.to_address.to_bytes_be().to_vec();
        let selector = value.selector.to_bytes_be().to_vec();
        let payload = value
            .payload
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let nonce = value
            .nonce
            .map(|n| n.to_bytes_be().to_vec())
            .unwrap_or_default();
        L1ToL2Message {
            from_address,
            to_address,
            selector,
            payload,
            nonce,
        }
    }
}

impl From<&sn_types::L2ToL1Message> for L2ToL1Message {
    fn from(value: &sn_types::L2ToL1Message) -> Self {
        let from_address = value.from_address.to_bytes_be().to_vec();
        let to_address = value.to_address.as_bytes().to_vec();
        let payload = value
            .payload
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        L2ToL1Message {
            from_address,
            to_address,
            payload,
        }
    }
}

impl From<&sn_types::Event> for Event {
    fn from(value: &sn_types::Event) -> Self {
        let from_address = value.from_address.to_bytes_be().to_vec();
        let keys = value
            .keys
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let data = value
            .data
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        Event {
            from_address,
            keys,
            data,
        }
    }
}

impl fmt::Debug for BlockBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockBuilder")
    }
}
