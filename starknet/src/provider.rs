//! Connect to the sequencer gateway.
use starknet::{
    core::types::{FieldElement, FromByteArrayError},
    providers::jsonrpc,
};
use url::Url;

use crate::core::{pb::v1alpha2, BlockHash, GlobalBlockId, InvalidBlockHashSize};

#[derive(Debug, Clone)]
pub enum BlockId {
    Latest,
    Pending,
    Hash(BlockHash),
    Number(u64),
}

#[apibara_node::async_trait]
pub trait Provider {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get the most recent accepted block number and hash.
    async fn get_head(&self) -> Result<GlobalBlockId, Self::Error>;

    /// Get a specific block.
    async fn get_block(&self, id: &BlockId) -> Result<v1alpha2::Block, Self::Error>;

    /// Get state update for a specific block.
    async fn get_state_update(&self, id: &BlockId) -> Result<v1alpha2::StateUpdate, Self::Error>;

    /// Get receipt for a specific transaction.
    async fn get_transaction_receipt(
        &self,
        hash: &[u8],
    ) -> Result<v1alpha2::TransactionReceipt, Self::Error>;
}

/// StarkNet RPC provider over HTTP.
pub struct HttpProvider {
    provider: jsonrpc::JsonRpcClient<jsonrpc::HttpTransport>,
}

#[derive(Debug, thiserror::Error)]
pub enum HttpProviderError {
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
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?;
        let hash = (&hash_and_number.block_hash).try_into()?;
        Ok(GlobalBlockId::new(hash_and_number.block_number, hash))
    }

    #[tracing::instrument(skip(self))]
    async fn get_block(&self, id: &BlockId) -> Result<v1alpha2::Block, Self::Error> {
        let block_id = id.try_into()?;
        let block = self
            .provider
            .get_block_with_txs(&block_id)
            .await
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?;

        match block {
            jsonrpc::models::MaybePendingBlockWithTxs::Block(block) => {
                if id.is_pending() {
                    return Err(HttpProviderError::UnexpectedPendingBlock);
                }
                let block = block.into();
                Ok(block)
            }
            jsonrpc::models::MaybePendingBlockWithTxs::PendingBlock(_block) => {
                if !id.is_pending() {
                    return Err(HttpProviderError::ExpectedPendingBlock);
                }
                todo!()
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
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?
            .into();
        Ok(state_update)
    }

    #[tracing::instrument(skip(self, hash))]
    async fn get_transaction_receipt(
        &self,
        hash: &[u8],
    ) -> Result<v1alpha2::TransactionReceipt, Self::Error> {
        let hash: FieldElement = TransactionHash(hash)
            .try_into()
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?;
        let receipt = self
            .provider
            .get_transaction_receipt(hash)
            .await
            .map_err(|err| HttpProviderError::Provider(Box::new(err)))?
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

impl From<jsonrpc::models::BlockWithTxs> for v1alpha2::Block {
    fn from(block: jsonrpc::models::BlockWithTxs) -> Self {
        let status: v1alpha2::BlockStatus = block.status.into();
        let header = block.header.into();
        let transactions = block.transactions.iter().map(Into::into).collect();

        v1alpha2::Block {
            status: status as i32,
            header: Some(header),
            transactions,
            receipts: Vec::default(),
            state_update: None,
        }
    }
}

impl From<jsonrpc::models::BlockStatus> for v1alpha2::BlockStatus {
    fn from(status: jsonrpc::models::BlockStatus) -> Self {
        use jsonrpc::models::BlockStatus;

        match status {
            BlockStatus::Pending => v1alpha2::BlockStatus::Pending,
            BlockStatus::AcceptedOnL2 => v1alpha2::BlockStatus::AcceptedOnL2,
            BlockStatus::AcceptedOnL1 => v1alpha2::BlockStatus::AcceptedOnL1,
            BlockStatus::Rejected => v1alpha2::BlockStatus::Rejected,
        }
    }
}

impl From<jsonrpc::models::BlockHeader> for v1alpha2::BlockHeader {
    fn from(header: jsonrpc::models::BlockHeader) -> Self {
        let block_hash = header.block_hash.into();
        let parent_block_hash = header.parent_hash.into();
        let block_number = header.block_number;
        let sequencer_address = header.sequencer_address.to_bytes_be().to_vec();
        let new_root = header.new_root.to_bytes_be().to_vec();
        let timestamp = prost_types::Timestamp {
            nanos: 0,
            seconds: header.timestamp as i64,
        };
        v1alpha2::BlockHeader {
            block_hash: Some(block_hash),
            parent_block_hash: Some(parent_block_hash),
            block_number,
            sequencer_address,
            new_root,
            timestamp: Some(timestamp),
        }
    }
}

impl From<FieldElement> for v1alpha2::BlockHash {
    fn from(value: FieldElement) -> Self {
        let hash = value.to_bytes_be().to_vec();
        v1alpha2::BlockHash { hash }
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
        use jsonrpc::models::InvokeTransactionVersion;
        use v1alpha2::transaction::Transaction;

        let meta: v1alpha2::TransactionMeta = (&tx.meta).into();
        match &tx.invoke_transaction {
            InvokeTransactionVersion::V0(v0) => {
                let contract_address = v0.contract_address.to_bytes_be().to_vec();
                let entry_point_selector = v0.entry_point_selector.to_bytes_be().to_vec();
                let calldata = v0
                    .calldata
                    .iter()
                    .map(|fe| fe.to_bytes_be().to_vec())
                    .collect();
                let invoke_v0 = v1alpha2::InvokeTransactionV0 {
                    contract_address,
                    entry_point_selector,
                    calldata,
                };
                v1alpha2::Transaction {
                    meta: Some(meta),
                    transaction: Some(Transaction::InvokeV0(invoke_v0)),
                }
            }
            InvokeTransactionVersion::V1(v1) => {
                let sender_address = v1.sender_address.to_bytes_be().to_vec();
                let calldata = v1
                    .calldata
                    .iter()
                    .map(|fe| fe.to_bytes_be().to_vec())
                    .collect();
                let invoke_v1 = v1alpha2::InvokeTransactionV1 {
                    sender_address,
                    calldata,
                };
                v1alpha2::Transaction {
                    meta: Some(meta),
                    transaction: Some(Transaction::InvokeV1(invoke_v1)),
                }
            }
        }
    }
}

impl From<&jsonrpc::models::DeployTransaction> for v1alpha2::Transaction {
    fn from(tx: &jsonrpc::models::DeployTransaction) -> Self {
        use v1alpha2::transaction::Transaction;

        let meta: v1alpha2::TransactionMeta = tx.into();
        let class_hash = tx.class_hash.to_bytes_be().to_vec();
        let contract_address_salt = tx
            .deploy_transaction_properties
            .contract_address_salt
            .to_bytes_be()
            .to_vec();
        let constructor_calldata = tx
            .deploy_transaction_properties
            .constructor_calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();

        let deploy = v1alpha2::DeployTransaction {
            class_hash,
            contract_address_salt,
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

        let meta: v1alpha2::TransactionMeta = (&tx.meta).into();
        let class_hash = tx.class_hash.to_bytes_be().to_vec();
        let sender_address = tx.sender_address.to_bytes_be().to_vec();

        let declare = v1alpha2::DeclareTransaction {
            class_hash,
            sender_address,
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

        let meta: v1alpha2::TransactionMeta = tx.into();
        let contract_address = tx.function_call.contract_address.to_bytes_be().to_vec();
        let entry_point_selector = tx.function_call.entry_point_selector.to_bytes_be().to_vec();
        let calldata = tx
            .function_call
            .calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();

        let l1_handler = v1alpha2::L1HandlerTransaction {
            contract_address,
            entry_point_selector,
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

        let meta: v1alpha2::TransactionMeta = (&tx.meta).into();
        let contract_address_salt = tx
            .deploy_properties
            .contract_address_salt
            .to_bytes_be()
            .to_vec();
        let class_hash = tx.deploy_properties.class_hash.to_bytes_be().to_vec();
        let constructor_calldata = tx
            .deploy_properties
            .constructor_calldata
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();

        let deploy_account = v1alpha2::DeployAccountTransaction {
            contract_address_salt,
            class_hash,
            constructor_calldata,
        };

        v1alpha2::Transaction {
            meta: Some(meta),
            transaction: Some(Transaction::DeployAccount(deploy_account)),
        }
    }
}

impl From<&jsonrpc::models::TransactionMeta> for v1alpha2::TransactionMeta {
    fn from(meta: &jsonrpc::models::TransactionMeta) -> Self {
        let hash = meta.transaction_hash.to_bytes_be().to_vec();
        let max_fee = meta.common_properties.max_fee.to_bytes_be().to_vec();
        let signature = meta
            .common_properties
            .signature
            .iter()
            .map(|fe| fe.to_bytes_be().to_vec())
            .collect();
        let version = meta.common_properties.version;
        let nonce = meta.common_properties.nonce.to_bytes_be().to_vec();
        v1alpha2::TransactionMeta {
            hash,
            max_fee,
            signature,
            nonce,
            version,
        }
    }
}

impl From<&jsonrpc::models::L1HandlerTransaction> for v1alpha2::TransactionMeta {
    fn from(tx: &jsonrpc::models::L1HandlerTransaction) -> Self {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let version = tx.version;
        let nonce = tx.nonce.to_be_bytes().to_vec();
        v1alpha2::TransactionMeta {
            hash,
            max_fee: Vec::default(),
            signature: Vec::default(),
            nonce,
            version,
        }
    }
}

impl From<&jsonrpc::models::DeployTransaction> for v1alpha2::TransactionMeta {
    fn from(tx: &jsonrpc::models::DeployTransaction) -> Self {
        let hash = tx.transaction_hash.to_bytes_be().to_vec();
        let version = tx.deploy_transaction_properties.version;
        v1alpha2::TransactionMeta {
            hash,
            max_fee: Vec::default(),
            signature: Vec::default(),
            nonce: Vec::default(),
            version,
        }
    }
}

impl From<jsonrpc::models::TransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: jsonrpc::models::TransactionReceipt) -> Self {
        use jsonrpc::models::TransactionReceipt;
        match &receipt {
            TransactionReceipt::Tagged(tagged) => tagged.into(),
            TransactionReceipt::Untagged(untagged) => untagged.into(),
        }
    }
}

impl From<&jsonrpc::models::TaggedTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: &jsonrpc::models::TaggedTransactionReceipt) -> Self {
        use jsonrpc::models::TaggedTransactionReceipt;

        match receipt {
            TaggedTransactionReceipt::Invoke(invoke) => (&invoke.meta).into(),
            TaggedTransactionReceipt::L1Handler(l1_handler) => (&l1_handler.meta).into(),
            TaggedTransactionReceipt::Declare(declare) => (&declare.meta).into(),
            TaggedTransactionReceipt::Deploy(deploy) => (&deploy.meta).into(),
            TaggedTransactionReceipt::DeployAccount(deploy) => (&deploy.meta).into(),
        }
    }
}

impl From<&jsonrpc::models::PendingTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: &jsonrpc::models::PendingTransactionReceipt) -> Self {
        use jsonrpc::models::PendingTransactionReceipt;

        match receipt {
            PendingTransactionReceipt::TransactionMeta(meta) => meta.into(),
            PendingTransactionReceipt::Deploy(deploy) => deploy.into(),
        }
    }
}

impl From<&jsonrpc::models::TransactionReceiptMeta> for v1alpha2::TransactionReceipt {
    fn from(meta: &jsonrpc::models::TransactionReceiptMeta) -> Self {
        let transaction_hash = meta.transaction_hash.to_bytes_be().to_vec();
        let actual_fee = meta.actual_fee.to_bytes_be().to_vec();
        let l2_to_l1_messages = meta.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = meta.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash,
            actual_fee,
            l2_to_l1_messages,
            events,
        }
    }
}

impl From<&jsonrpc::models::DeployTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: &jsonrpc::models::DeployTransactionReceipt) -> Self {
        // TODO: include deployed contract address
        (&receipt.meta).into()
    }
}

impl From<&jsonrpc::models::PendingTransactionReceiptMeta> for v1alpha2::TransactionReceipt {
    fn from(meta: &jsonrpc::models::PendingTransactionReceiptMeta) -> Self {
        let transaction_hash = meta.transaction_hash.to_bytes_be().to_vec();
        let actual_fee = meta.actual_fee.to_bytes_be().to_vec();
        let l2_to_l1_messages = meta.messages_sent.iter().map(|msg| msg.into()).collect();
        let events = meta.events.iter().map(|ev| ev.into()).collect();

        v1alpha2::TransactionReceipt {
            transaction_index: 0,
            transaction_hash,
            actual_fee,
            l2_to_l1_messages,
            events,
        }
    }
}

impl From<&jsonrpc::models::PendingDeployTransactionReceipt> for v1alpha2::TransactionReceipt {
    fn from(receipt: &jsonrpc::models::PendingDeployTransactionReceipt) -> Self {
        // TODO: include deployed contract address
        (&receipt.meta).into()
    }
}

impl From<&jsonrpc::models::MsgToL1> for v1alpha2::L2ToL1Message {
    fn from(msg: &jsonrpc::models::MsgToL1) -> Self {
        let to_address = msg.to_address.to_bytes_be().to_vec();
        let payload = msg
            .payload
            .iter()
            .map(|p| p.to_bytes_be().to_vec())
            .collect();

        v1alpha2::L2ToL1Message {
            to_address,
            payload,
        }
    }
}

impl From<&jsonrpc::models::Event> for v1alpha2::Event {
    fn from(event: &jsonrpc::models::Event) -> Self {
        let from_address = event.from_address.to_bytes_be().to_vec();
        let keys = event
            .content
            .keys
            .iter()
            .map(|k| k.to_bytes_be().to_vec())
            .collect();
        let data = event
            .content
            .data
            .iter()
            .map(|d| d.to_bytes_be().to_vec())
            .collect();

        v1alpha2::Event {
            from_address,
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
        let new_root = update.new_root.to_bytes_be().to_vec();
        let old_root = update.old_root.to_bytes_be().to_vec();
        let state_diff = update.state_diff.into();
        v1alpha2::StateUpdate {
            new_root,
            old_root,
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
        let contract_address = diff.address.to_bytes_be().to_vec();
        let storage_entries = diff.storage_entries.iter().map(|e| e.into()).collect();
        v1alpha2::StorageDiff {
            contract_address,
            storage_entries,
        }
    }
}

impl From<&jsonrpc::models::StorageEntry> for v1alpha2::StorageEntry {
    fn from(diff: &jsonrpc::models::StorageEntry) -> Self {
        let key = diff.key.to_bytes_be().to_vec();
        let value = diff.value.to_bytes_be().to_vec();
        v1alpha2::StorageEntry { key, value }
    }
}

impl From<&FieldElement> for v1alpha2::DeclaredContract {
    fn from(diff: &FieldElement) -> Self {
        let class_hash = diff.to_bytes_be().to_vec();
        v1alpha2::DeclaredContract { class_hash }
    }
}

impl From<&jsonrpc::models::DeployedContractItem> for v1alpha2::DeployedContract {
    fn from(diff: &jsonrpc::models::DeployedContractItem) -> Self {
        let contract_address = diff.address.to_bytes_be().to_vec();
        let class_hash = diff.class_hash.to_bytes_be().to_vec();
        v1alpha2::DeployedContract {
            contract_address,
            class_hash,
        }
    }
}

impl From<&jsonrpc::models::NonceUpdate> for v1alpha2::NonceUpdate {
    fn from(diff: &jsonrpc::models::NonceUpdate) -> Self {
        let contract_address = diff.contract_address.to_bytes_be().to_vec();
        let nonce = diff.nonce.to_bytes_be().to_vec();
        v1alpha2::NonceUpdate {
            contract_address,
            nonce,
        }
    }
}
