use std::{sync::Arc, time::Duration};

use backon::{ExponentialBuilder, Retryable};
use error_stack::{Report, Result, ResultExt};
use reqwest::header::{HeaderMap, HeaderValue};
use starknet_rust::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider};
use tracing::warn;
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
    /// Exponential backoff configuration.
    pub exponential_backoff: ExponentialBuilder,
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
    ) -> Result<models::MaybePreConfirmedBlockWithTxHashes, StarknetProviderError> {
        let starknet_block_id: starknet_rust::core::types::BlockId = block_id.into();

        let request = (|| async {
            self.client
                .get_block_with_tx_hashes(starknet_block_id)
                .await
        })
        .retry(self.options.exponential_backoff)
        .notify(|err, duration| {
            warn!(duration = ?duration, error = %err, block_id = ?block_id, "get_block_with_tx_hashes failed");
        });
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
    ) -> Result<models::MaybePreConfirmedBlockWithReceipts, StarknetProviderError> {
        let starknet_block_id: starknet_rust::core::types::BlockId = block_id.into();

        let request = (|| async { self.client.get_block_with_receipts(starknet_block_id, None).await })
            .retry(self.options.exponential_backoff)
            .notify(|err, duration| {
                warn!(duration = ?duration, error = %err, block_id = ?block_id, "get_block_with_receipts failed");
            });
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
    ) -> Result<models::MaybePreConfirmedStateUpdate, StarknetProviderError> {
        let starknet_block_id: starknet_rust::core::types::BlockId = block_id.into();

        let request = (|| async { self.client.get_state_update(starknet_block_id).await })
            .retry(self.options.exponential_backoff)
            .notify(|err, duration| {
                warn!(duration = ?duration, error = %err, block_id = ?block_id, "get_state_update failed");
            });
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
        let starknet_block_id: starknet_rust::core::types::ConfirmedBlockId =
            block_id.try_into()?;

        let request = (|| async {
            self.client
                .trace_block_transactions(starknet_block_id, None)
                .await
        })
        .retry(self.options.exponential_backoff)
        .notify(|err, duration| {
            warn!(duration = ?duration, error = %err, block_id = ?block_id, "trace_block_transactions failed");
        });
        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(StarknetProviderError::Timeout)
                .attach_printable("failed to get block traces")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        match response
            .or_else(convert_error)
            .attach_printable("failed to get block traces")
            .attach_printable_lazy(|| format!("block id: {block_id:?}"))?
        {
            models::TraceBlockTransactionsResult::Traces(traces) => Ok(traces),
            models::TraceBlockTransactionsResult::TracesWithInitialReads { traces, .. } => {
                Ok(traces)
            }
        }
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

impl From<&BlockId> for starknet_rust::core::types::BlockId {
    fn from(v: &BlockId) -> Self {
        match v {
            BlockId::Head => starknet_rust::core::types::BlockId::Tag(
                starknet_rust::core::types::BlockTag::Latest,
            ),
            BlockId::Pending => starknet_rust::core::types::BlockId::Tag(
                starknet_rust::core::types::BlockTag::PreConfirmed,
            ),
            BlockId::Number(number) => starknet_rust::core::types::BlockId::Number(*number),
            BlockId::Hash(hash) => starknet_rust::core::types::BlockId::Hash(*hash),
        }
    }
}

impl TryFrom<&BlockId> for starknet_rust::core::types::ConfirmedBlockId {
    type Error = Report<StarknetProviderError>;

    fn try_from(v: &BlockId) -> std::result::Result<Self, Self::Error> {
        match v {
            BlockId::Head => Ok(starknet_rust::core::types::ConfirmedBlockId::Latest),
            BlockId::Pending => Err(StarknetProviderError::Request)
                .attach_printable("expected a confirmed block id, got pending"),
            BlockId::Number(number) => Ok(starknet_rust::core::types::ConfirmedBlockId::Number(
                *number,
            )),
            BlockId::Hash(hash) => Ok(starknet_rust::core::types::ConfirmedBlockId::Hash(*hash)),
        }
    }
}

fn convert_error<T>(
    err: starknet_rust::providers::ProviderError,
) -> Result<T, StarknetProviderError> {
    use starknet_rust::core::types::StarknetError as SNError;
    use starknet_rust::providers::ProviderError;

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
