//! Connect to the sequencer gateway.
use starknet::{
    core::types::{FieldElement, FromByteArrayError},
    providers::jsonrpc::{self, models::ErrorCode, JsonRpcClientError, RpcError},
};
use url::Url;

use crate::core::{pb::starknet::v1alpha2, BlockHash, GlobalBlockId, InvalidBlockHashSize};

#[derive(Debug, Clone)]
pub enum BlockId {
    Latest,
    Pending,
    Hash(BlockHash),
    Number(u64),
}

pub trait ProviderError: std::error::Error + Send + Sync + 'static {
    fn is_block_not_found(&self) -> bool;
}

#[apibara_node::async_trait]
pub trait Provider {
    type Error: ProviderError;

    /// Get the most recent accepted block number and hash.
    async fn get_head(&self) -> Result<GlobalBlockId, Self::Error>;

    /// Get a specific block.
    async fn get_block(
        &self,
        id: &BlockId,
    ) -> Result<
        (
            v1alpha2::BlockStatus,
            v1alpha2::BlockHeader,
            v1alpha2::BlockBody,
        ),
        Self::Error,
    >;

    /// Get state update for a specific block.
    async fn get_state_update(&self, id: &BlockId) -> Result<v1alpha2::StateUpdate, Self::Error>;

    /// Get receipt for a specific transaction.
    async fn get_transaction_receipt(
        &self,
        hash: &v1alpha2::FieldElement,
    ) -> Result<v1alpha2::TransactionReceipt, Self::Error>;
}

/// StarkNet RPC provider over HTTP.
pub struct HttpProvider {
    provider: jsonrpc::JsonRpcClient<jsonrpc::HttpTransport>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpProviderError {
    #[error("the given block was not found")]
    BlockNotFound,
    #[error("failed to parse gateway configuration")]
    Configuration,
    #[error("failed to parse gateway url")]
    Url(#[from] url::ParseError),
    #[error(transparent)]
    Provider(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("received unexpected pending block")]
    UnexpectedPendingBlock,
    #[error("expected pending block, but received non pending block")]
    ExpectedPendingBlock,
    #[error("failed to parse block id")]
    InvalidBlockId(#[from] FromByteArrayError),
    #[error("failed to parse block hash")]
    InvalidBlockHash(#[from] InvalidBlockHashSize),
}

impl HttpProvider {
    pub fn new(rpc_url: Url) -> Self {
        let http = jsonrpc::HttpTransport::new(rpc_url);
        let provider = jsonrpc::JsonRpcClient::new(http);
        HttpProvider { provider }
    }
}

impl ProviderError for HttpProviderError {
    fn is_block_not_found(&self) -> bool {
        matches!(self, HttpProviderError::BlockNotFound)
    }
}

impl HttpProviderError {
    pub fn from_provider_error<T>(error: JsonRpcClientError<T>) -> HttpProviderError
    where
        T: std::error::Error + Send + Sync + 'static,
    {
        match error {
            JsonRpcClientError::RpcError(RpcError::Code(ErrorCode::BlockNotFound)) => {
                HttpProviderError::BlockNotFound
            }
            _ => HttpProviderError::Provider(Box::new(error)),
        }
    }
}

struct TransactionHash<'a>(&'a [u8]);

#[apibara_node::async_trait]
impl Provider for HttpProvider {
    type Error = HttpProviderError;

    #[tracing::instrument(skip(self))]
    async fn get_head(&self) -> Result<GlobalBlockId, Self::Error> {
        let hash_and_number = self
            .provider
            .block_hash_and_number()
            .await
            .map_err(HttpProviderError::from_provider_error)?;
        let hash: v1alpha2::FieldElement = hash_and_number.block_hash.into();
        Ok(GlobalBlockId::new(
            hash_and_number.block_number,
            hash.into(),
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn get_block(
        &self,
        id: &BlockId,
    ) -> Result<
        (
            v1alpha2::BlockStatus,
            v1alpha2::BlockHeader,
            v1alpha2::BlockBody,
        ),
        Self::Error,
    > {
        let block_id = id.try_into()?;
        let block = self
            .provider
            .get_block_with_txs(&block_id)
            .await
            .map_err(HttpProviderError::from_provider_error)?;

        match block {
            jsonrpc::models::MaybePendingBlockWithTxs::Block(ref block) => {
                if id.is_pending() {
                    return Err(HttpProviderError::UnexpectedPendingBlock);
                }
                let status = block.into();
                let header = block.into();
                let body = block.into();
                Ok((status, header, body))
            }
            jsonrpc::models::MaybePendingBlockWithTxs::PendingBlock(ref block) => {
                if !id.is_pending() {
                    return Err(HttpProviderError::ExpectedPendingBlock);
                }
                let status = block.into();
                let header = block.into();
                let body = block.into();
                Ok((status, header, body))
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_state_update(&self, id: &BlockId) -> Result<v1alpha2::StateUpdate, Self::Error> {
        let block_id = id.try_into()?;
        let state_update = self
            .provider
            .get_state_update(&block_id)
            .await
            .map_err(HttpProviderError::from_provider_error)?
            .into();
        Ok(state_update)
    }

    #[tracing::instrument(skip(self), fields(hash = %hash))]
    async fn get_transaction_receipt(
        &self,
        hash: &v1alpha2::FieldElement,
    ) -> Result<v1alpha2::TransactionReceipt, Self::Error> {
        let hash: FieldElement = hash
            .try_into()
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?;
        let receipt = self
            .provider
            .get_transaction_receipt(hash)
            .await
            .map_err(HttpProviderError::from_provider_error)?
            .into();
        Ok(receipt)
    }
}

impl BlockId {
    pub fn is_pending(&self) -> bool {
        matches!(self, BlockId::Pending)
    }
}

impl TryFrom<&BlockId> for jsonrpc::models::BlockId {
    type Error = FromByteArrayError;

    fn try_from(value: &BlockId) -> Result<Self, Self::Error> {
        use jsonrpc::models::{BlockId as SNBlockId, BlockTag};

        match value {
            BlockId::Latest => Ok(SNBlockId::Tag(BlockTag::Latest)),
            BlockId::Pending => Ok(SNBlockId::Tag(BlockTag::Pending)),
            BlockId::Hash(hash) => {
                let hash = hash.try_into()?;
                Ok(SNBlockId::Hash(hash))
            }
            BlockId::Number(number) => Ok(SNBlockId::Number(*number)),
        }
    }
}

impl From<&jsonrpc::models::BlockWithTxs> for v1alpha2::BlockStatus {
    fn from(block: &jsonrpc::models::BlockWithTxs) -> Self {
        (&block.status).into()
    }
}

impl From<&jsonrpc::models::PendingBlockWithTxs> for v1alpha2::BlockStatus {
    fn from(_block: &jsonrpc::models::PendingBlockWithTxs) -> Self {
        v1alpha2::BlockStatus::Pending
    }
}

impl From<&jsonrpc::models::BlockWithTxs> for v1alpha2::BlockHeader {
    fn from(block: &jsonrpc::models::BlockWithTxs) -> Self {
        let block_hash = block.block_hash.into();
        let parent_block_hash = block.parent_hash.into();
        let block_number = block.block_number;
        let sequencer_address = block.sequencer_address.into();
        let new_root = block.new_root.into();
        let timestamp = prost_types::Timestamp {
            nanos: 0,
            seconds: block.timestamp as i64,
        };

        v1alpha2::BlockHeader {
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
            block_number,
            sequencer_address: Some(sequencer_address),
            new_root: Some(new_root),
            timestamp: Some(timestamp),
        }
    }
}

impl From<&jsonrpc::models::PendingBlockWithTxs> for v1alpha2::BlockHeader {
    fn from(block: &jsonrpc::models::PendingBlockWithTxs) -> Self {
        let block_hash = FieldElement::ZERO.into();
        let parent_block_hash = block.parent_hash.into();
        let sequencer_address = block.sequencer_address.into();
        let timestamp = prost_types::Timestamp {
            nanos: 0,
            seconds: block.timestamp as i64,
        };

        v1alpha2::BlockHeader {
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
            block_number: u64::MAX,
            sequencer_address: Some(sequencer_address),
            new_root: None,
            timestamp: Some(timestamp),
        }
    }
}

impl From<&jsonrpc::models::BlockWithTxs> for v1alpha2::BlockBody {
    fn from(block: &jsonrpc::models::BlockWithTxs) -> Self {
        let transactions = block.transactions.iter().map(Into::into).collect();
        v1alpha2::BlockBody { transactions }
    }
}

impl From<&jsonrpc::models::PendingBlockWithTxs> for v1alpha2::BlockBody {
    fn from(block: &jsonrpc::models::PendingBlockWithTxs) -> Self {
        let transactions = block.transactions.iter().map(Into::into).collect();
        v1alpha2::BlockBody { transactions }
    }
}

impl From<&jsonrpc::models::BlockStatus> for v1alpha2::BlockStatus {
    fn from(status: &jsonrpc::models::BlockStatus) -> Self {
        use jsonrpc::models::BlockStatus;

        match status {
            BlockStatus::Pending => v1alpha2::BlockStatus::Pending,
            BlockStatus::AcceptedOnL2 => v1alpha2::BlockStatus::AcceptedOnL2,
            BlockStatus::AcceptedOnL1 => v1alpha2::BlockStatus::AcceptedOnL1,
            BlockStatus::Rejected => v1alpha2::BlockStatus::Rejected,
        }
    }
}

impl From<FieldElement> for v1alpha2::FieldElement {
    fn from(felt: FieldElement) -> Self {
        (&felt).into()
    }
}

impl From<&FieldElement> for v1alpha2::FieldElement {
    fn from(felt: &FieldElement) -> Self {
        let bytes = felt.to_bytes_be();
        v1alpha2::FieldElement::from_bytes(&bytes)
    }
}

impl From<&jsonrpc::models::Transaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::Transaction) -> Self {
        use jsonrpc::models::Transaction;
        match tx {
            Transaction::Invoke(invoke) => invoke.into(),
            Transaction::Deploy(deploy) => deploy.into(),
            Transaction::Declare(declare) => declare.into(),
            Transaction::L1Handler(l1_handler) => l1_handler.into(),
            Transaction::DeployAccount(deploy_account) => deploy_account.into(),
        }
    }
}

impl From<&jsonrpc::models::InvokeTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::InvokeTransaction) -> Self {
        use jsonrpc::models::InvokeTransaction;

        match &tx {
            InvokeTransaction::V0(v0) => v0.into(),
            InvokeTransaction::V1(v1) => v1.into(),
        }
    }
}

impl From<&jsonrpc::models::InvokeTransactionV0> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::InvokeTransactionV0) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();
        let max_fee = tx.max_fee.into();
        let signature = tx.signature.iter().map(|fe| fe.into()).collect();
        let nonce = tx.nonce.into();

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            version: 0,
        };

        let contract_address = tx.contract_address.into();
        let entry_point_selector = tx.entry_point_selector.into();
        let calldata = tx.calldata.iter().map(|fe| fe.into()).collect();

        let invoke_v0 = v1alpha2::InvokeTransactionV0 {
            contract_address: Some(contract_address),
            entry_point_selector: Some(entry_point_selector),
            calldata,
        };
        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::InvokeV0(invoke_v0)),
        }
    }
}

impl From<&jsonrpc::models::InvokeTransactionV1> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::InvokeTransactionV1) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();
        let max_fee = tx.max_fee.into();
        let signature = tx.signature.iter().map(|fe| fe.into()).collect();
        let nonce = tx.nonce.into();

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            version: 0,
        };

        let sender_address = tx.sender_address.into();
        let calldata = tx.calldata.iter().map(|fe| fe.into()).collect();
        let invoke_v1 = v1alpha2::InvokeTransactionV1 {
            sender_address: Some(sender_address),
            calldata,
        };
        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::InvokeV1(invoke_v1)),
        }
    }
}

impl From<&jsonrpc::models::DeployTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::DeployTransaction) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            version: tx.version,
            ..v1alpha2::TransactionMeta::default()
        };

        let class_hash = tx.class_hash.into();
        let contract_address_salt = tx.contract_address_salt.into();
        let constructor_calldata = tx.constructor_calldata.iter().map(|fe| fe.into()).collect();

        let deploy = v1alpha2::DeployTransaction {
            class_hash: Some(class_hash),
            contract_address_salt: Some(contract_address_salt),
            constructor_calldata,
        };

        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::Deploy(deploy)),
        }
    }
}

impl From<&jsonrpc::models::DeclareTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::DeclareTransaction) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();
        let max_fee = tx.max_fee.into();
        let signature = tx.signature.iter().map(|fe| fe.into()).collect();
        let nonce = tx.nonce.into();
        let version = tx.version;

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            version,
        };

        let class_hash = tx.class_hash.into();
        let sender_address = tx.sender_address.into();

        let declare = v1alpha2::DeclareTransaction {
            class_hash: Some(class_hash),
            sender_address: Some(sender_address),
        };

        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::Declare(declare)),
        }
    }
}

impl From<&jsonrpc::models::L1HandlerTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::L1HandlerTransaction) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();
        let version = tx.version;

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            version,
            ..v1alpha2::TransactionMeta::default()
        };

        let contract_address = tx.contract_address.into();
        let entry_point_selector = tx.entry_point_selector.into();
        let calldata = tx.calldata.iter().map(|fe| fe.into()).collect();

        let l1_handler = v1alpha2::L1HandlerTransaction {
            contract_address: Some(contract_address),
            entry_point_selector: Some(entry_point_selector),
            calldata,
        };

        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::L1Handler(l1_handler)),
        }
    }
}

impl From<&jsonrpc::models::DeployAccountTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::DeployAccountTransaction) -> Self {
        use v1alpha2::transaction::Transaction;

        let hash = tx.transaction_hash.into();
        let max_fee = tx.max_fee.into();
        let signature = tx.signature.iter().map(|fe| fe.into()).collect();
        let nonce = tx.nonce.into();
        let version = tx.version;

        let meta = v1alpha2::TransactionMeta {
            hash: Some(hash),
            max_fee: Some(max_fee),
            signature,
            nonce: Some(nonce),
            version,
        };

        let contract_address_salt = tx.contract_address_salt.into();
        let class_hash = tx.class_hash.into();
        let constructor_calldata = tx.constructor_calldata.iter().map(|fe| fe.into()).collect();

        let deploy_account = v1alpha2::DeployAccountTransaction {
            contract_address_salt: Some(contract_address_salt),
            class_hash: Some(class_hash),
            constructor_calldata,
        };

        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::DeployAccount(deploy_account)),
        }
    }
}

impl From<jsonrpc::models::MaybePendingTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::MaybePendingTransactionReceipt) -> Self {
        use jsonrpc::models::MaybePendingTransactionReceipt;

        match receipt {
            MaybePendingTransactionReceipt::PendingReceipt(receipt) => receipt.into(),
            MaybePendingTransactionReceipt::Receipt(receipt) => receipt.into(),
        }
    }
}

impl From<jsonrpc::models::PendingTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::PendingTransactionReceipt) -> Self {
        use jsonrpc::models::PendingTransactionReceipt;

        match receipt {
            PendingTransactionReceipt::Invoke(invoke) => invoke.into(),
            PendingTransactionReceipt::L1Handler(l1_handler) => l1_handler.into(),
            PendingTransactionReceipt::Declare(declare) => declare.into(),
            PendingTransactionReceipt::Deploy(deploy) => deploy.into(),
            PendingTransactionReceipt::DeployAccount(deploy) => deploy.into(),
        }
    }
}

impl From<jsonrpc::models::PendingInvokeTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::PendingInvokeTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::PendingL1HandlerTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::PendingL1HandlerTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::PendingDeclareTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::PendingDeclareTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::PendingDeployTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::PendingDeployTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();
        let contract_address = receipt.contract_address.into();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: Some(contract_address),
        }
    }
}

impl From<jsonrpc::models::PendingDeployAccountTransactionReceipt>
    for v1alpha2::TransactionReceipt
{
    fn from(receipt: jsonrpc::models::PendingDeployAccountTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::TransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::TransactionReceipt) -> Self {
        use jsonrpc::models::TransactionReceipt;

        match receipt {
            TransactionReceipt::Invoke(invoke) => invoke.into(),
            TransactionReceipt::L1Handler(l1_handler) => l1_handler.into(),
            TransactionReceipt::Declare(declare) => declare.into(),
            TransactionReceipt::Deploy(deploy) => deploy.into(),
            TransactionReceipt::DeployAccount(deploy) => deploy.into(),
        }
    }
}

impl From<jsonrpc::models::InvokeTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::InvokeTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::L1HandlerTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::L1HandlerTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::DeclareTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::DeclareTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: None,
        }
    }
}

impl From<jsonrpc::models::DeployTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::DeployTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();
        let contract_address = receipt.contract_address.into();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: Some(contract_address),
        }
    }
}

impl From<jsonrpc::models::DeployAccountTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::DeployAccountTransactionReceipt) -> Self {
        let transaction_hash = receipt.transaction_hash.into();
        let actual_fee = receipt.actual_fee.into();
        let l2_to_l1_messages = receipt.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = receipt.events.iter().map(|ev| ev.into()).collect();
        let contract_address = receipt.contract_address.into();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash: Some(transaction_hash),
            actual_fee: Some(actual_fee),
            l2_to_l1_messages,
            events,
            contract_address: Some(contract_address),
        }
    }
}

impl From<&jsonrpc::models::MsgToL1> for v1alpha2::L2ToL1Message {
    fn from(msg: &jsonrpc::models::MsgToL1) -> Self {
        let to_address = msg.to_address.into();
        let payload = msg.payload.iter().map(|p| p.into()).collect();

        v1alpha2::L2ToL1Message {
            to_address: Some(to_address),
            payload,
        }
    }
}

impl From<&jsonrpc::models::Event> for v1alpha2::Event {
    fn from(event: &jsonrpc::models::Event) -> Self {
        let from_address = event.from_address.into();
        let keys = event.keys.iter().map(|k| k.into()).collect();
        let data = event.data.iter().map(|d| d.into()).collect();

        v1alpha2::Event {
            from_address: Some(from_address),
            keys,
            data,
        }
    }
}

impl<'a> TryFrom<TransactionHash<'a>> for FieldElement {
    type Error = FromByteArrayError;

    fn try_from(value: TransactionHash<'a>) -> Result<Self, Self::Error> {
        let hash_len = value.0.len();
        if hash_len > 32 {
            return Err(FromByteArrayError);
        }
        let mut buf = [0; 32];
        buf[..hash_len].copy_from_slice(value.0);
        FieldElement::from_bytes_be(&buf)
    }
}

impl From<jsonrpc::models::StateUpdate> for v1alpha2::StateUpdate {
    fn from(update: jsonrpc::models::StateUpdate) -> Self {
        let new_root = update.new_root.into();
        let old_root = update.old_root.into();
        let state_diff = update.state_diff.into();
        v1alpha2::StateUpdate {
            new_root: Some(new_root),
            old_root: Some(old_root),
            state_diff: Some(state_diff),
        }
    }
}

impl From<jsonrpc::models::StateDiff> for v1alpha2::StateDiff {
    fn from(diff: jsonrpc::models::StateDiff) -> Self {
        let storage_diffs = diff.storage_diffs.iter().map(|d| d.into()).collect();
        let declared_contracts = diff
            .declared_contract_hashes
            .iter()
            .map(|d| d.into())
            .collect();
        let deployed_contracts = diff.deployed_contracts.iter().map(|d| d.into()).collect();
        let nonces = diff.nonces.iter().map(|d| d.into()).collect();
        v1alpha2::StateDiff {
            storage_diffs,
            declared_contracts,
            deployed_contracts,
            nonces,
        }
    }
}

impl From<&jsonrpc::models::ContractStorageDiffItem> for v1alpha2::StorageDiff {
    fn from(diff: &jsonrpc::models::ContractStorageDiffItem) -> Self {
        let contract_address = diff.address.into();
        let storage_entries = diff.storage_entries.iter().map(|e| e.into()).collect();
        v1alpha2::StorageDiff {
            contract_address: Some(contract_address),
            storage_entries,
        }
    }
}

impl From<&jsonrpc::models::StorageEntry> for v1alpha2::StorageEntry {
    fn from(diff: &jsonrpc::models::StorageEntry) -> Self {
        let key = diff.key.into();
        let value = diff.value.into();
        v1alpha2::StorageEntry {
            key: Some(key),
            value: Some(value),
        }
    }
}

impl From<&FieldElement> for v1alpha2::DeclaredContract {
    fn from(diff: &FieldElement) -> Self {
        let class_hash = diff.into();
        v1alpha2::DeclaredContract {
            class_hash: Some(class_hash),
        }
    }
}

impl From<&jsonrpc::models::DeployedContractItem> for v1alpha2::DeployedContract {
    fn from(diff: &jsonrpc::models::DeployedContractItem) -> Self {
        let contract_address = diff.address.into();
        let class_hash = diff.class_hash.into();
        v1alpha2::DeployedContract {
            contract_address: Some(contract_address),
            class_hash: Some(class_hash),
        }
    }
}

impl From<&jsonrpc::models::NonceUpdate> for v1alpha2::NonceUpdate {
    fn from(diff: &jsonrpc::models::NonceUpdate) -> Self {
        let contract_address = diff.contract_address.into();
        let nonce = diff.nonce.into();
        v1alpha2::NonceUpdate {
            contract_address: Some(contract_address),
            nonce: Some(nonce),
        }
    }
}
