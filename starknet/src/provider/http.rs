use std::{sync::Arc, time::Duration};

use error_stack::{Report, Result, ResultExt};
use reqwest::header::{HeaderMap, HeaderValue};
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use url::Url;

use super::models;

#[derive(Debug)]
pub enum StarknetProviderError {
    Request,
    Timeout,
    NotFound,
    Configuration,
}

#[derive(Debug, Clone)]
pub enum BlockId {
    /// Head block.
    Head,
    /// Pending block.
    Pending,
    /// Block by number.
    Number(u64),
    /// Block by hash.
    Hash(models::FieldElement),
}

#[derive(Debug, Clone)]
pub struct StarknetProviderOptions {
    /// Request timeout.
    pub timeout: Duration,
    /// Request headers.
    pub headers: HeaderMap<HeaderValue>,
}

#[derive(Clone)]
pub struct StarknetProvider {
    client: Arc<JsonRpcClient<HttpTransport>>,
    options: StarknetProviderOptions,
}

pub trait StarknetProviderErrorExt {
    fn is_not_found(&self) -> bool;
}

impl StarknetProvider {
    pub fn new(url: Url, options: StarknetProviderOptions) -> Result<Self, StarknetProviderError> {
        let mut transport = HttpTransport::new(url);
        for (key, value) in options.headers.iter() {
            let key = key.to_string();
            let value = value
                .to_str()
                .change_context(StarknetProviderError::Configuration)
                .attach_printable("failed to convert header value to string")?
                .to_string();
            transport.add_header(key, value);
        }

        let client = JsonRpcClient::new(transport).into();

        Ok(Self { client, options })
    }

    pub async fn get_block_with_tx_hashes(
        &self,
        block_id: &BlockId,
    ) -> Result<models::MaybePendingBlockWithTxHashes, StarknetProviderError> {
        let starknet_block_id: starknet::core::types::BlockId = block_id.into();

        let request = self.client.get_block_with_tx_hashes(starknet_block_id);
        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(StarknetProviderError::Timeout)
                .attach_printable("failed to get block with transaction hashes")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .or_else(convert_error)
            .attach_printable("failed to get block with transaction hashes")
            .attach_printable_lazy(|| format!("block id: {block_id:?}"))
    }

    pub async fn get_block_with_receipts(
        &self,
        block_id: &BlockId,
    ) -> Result<models::MaybePendingBlockWithReceipts, StarknetProviderError> {
        let starknet_block_id: starknet::core::types::BlockId = block_id.into();

        let request = self.client.get_block_with_receipts(starknet_block_id);
        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(StarknetProviderError::Timeout)
                .attach_printable("failed to get block with receipts")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .or_else(convert_error)
            .attach_printable("failed to get block with receipts")
            .attach_printable_lazy(|| format!("block id: {block_id:?}"))
    }

    pub async fn get_state_update(
        &self,
        block_id: &BlockId,
    ) -> Result<models::MaybePendingStateUpdate, StarknetProviderError> {
        let starknet_block_id: starknet::core::types::BlockId = block_id.into();

        let request = self.client.get_state_update(starknet_block_id);
        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(StarknetProviderError::Timeout)
                .attach_printable("failed to get block state update")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .or_else(convert_error)
            .attach_printable("failed to get block state update")
            .attach_printable_lazy(|| format!("block id: {block_id:?}"))
    }

    pub async fn get_block_transaction_traces(
        &self,
        block_id: &BlockId,
    ) -> Result<Vec<models::TransactionTraceWithHash>, StarknetProviderError> {
        let starknet_block_id: starknet::core::types::BlockId = block_id.into();

        let request = self.client.trace_block_transactions(starknet_block_id);
        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(StarknetProviderError::Timeout)
                .attach_printable("failed to get block traces")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .or_else(convert_error)
            .attach_printable("failed to get block traces")
            .attach_printable_lazy(|| format!("block id: {block_id:?}"))
    }
}

impl error_stack::Context for StarknetProviderError {}

impl std::fmt::Display for StarknetProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StarknetProviderError::Request => write!(f, "failed to send request"),
            StarknetProviderError::Timeout => write!(f, "request timed out"),
            StarknetProviderError::NotFound => write!(f, "not found"),
            StarknetProviderError::Configuration => write!(f, "configuration error"),
        }
    }
}

impl From<&BlockId> for starknet::core::types::BlockId {
    fn from(v: &BlockId) -> Self {
        match v {
            BlockId::Head => {
                starknet::core::types::BlockId::Tag(starknet::core::types::BlockTag::Latest)
            }
            BlockId::Pending => {
                starknet::core::types::BlockId::Tag(starknet::core::types::BlockTag::Pending)
            }
            BlockId::Number(number) => starknet::core::types::BlockId::Number(*number),
            BlockId::Hash(hash) => starknet::core::types::BlockId::Hash(*hash),
        }
    }
}

fn convert_error<T>(err: starknet::providers::ProviderError) -> Result<T, StarknetProviderError> {
    use starknet::core::types::StarknetError as SNError;
    use starknet::providers::ProviderError;

    match err {
        ProviderError::StarknetError(SNError::BlockNotFound) => {
            Err(err).change_context(StarknetProviderError::NotFound)
        }
        _ => Err(err).change_context(StarknetProviderError::Request),
    }
}

impl StarknetProviderErrorExt for Report<StarknetProviderError> {
    fn is_not_found(&self) -> bool {
        matches!(self.current_context(), StarknetProviderError::NotFound)
    }
}
