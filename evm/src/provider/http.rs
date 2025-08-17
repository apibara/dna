use std::{sync::Arc, time::Duration};

use alloy_primitives::BlockHash;
use alloy_provider::{ext::TraceApi, network::Ethereum, Provider, ProviderBuilder};
use alloy_rpc_client::ClientBuilder;
use backon::{ExponentialBuilder, Retryable};
use error_stack::{Report, Result, ResultExt};
use reqwest::header::{HeaderMap, HeaderValue};
use url::Url;

pub use alloy_rpc_types::BlockId;

use super::models;

#[derive(Debug)]
pub enum JsonRpcProviderError {
    Request,
    Timeout,
    NotFound,
    Configuration,
}

#[derive(Debug, Clone)]
pub struct JsonRpcProviderOptions {
    /// Request timeout.
    pub timeout: Duration,
    /// Request headers.
    pub headers: HeaderMap<HeaderValue>,
    /// Exponential backoff options.
    pub exponential_backoff: ExponentialBuilder,
}

pub trait ProviderWithTraceApi: Provider<Ethereum> + TraceApi<Ethereum> {}

impl<T> ProviderWithTraceApi for T where T: Provider<Ethereum> + TraceApi<Ethereum> {}

#[derive(Clone)]
pub struct JsonRpcProvider {
    provider: Arc<dyn ProviderWithTraceApi>,
    options: JsonRpcProviderOptions,
}

pub trait JsonRpcProviderErrorExt {
    fn is_not_found(&self) -> bool;
}

impl JsonRpcProvider {
    pub fn new(url: Url, options: JsonRpcProviderOptions) -> Result<Self, JsonRpcProviderError> {
        if !options.headers.is_empty() {
            return Err(JsonRpcProviderError::Configuration)
                .attach_printable("custom headers are not supported");
        }

        let client = ClientBuilder::default().http(url);
        let provider = ProviderBuilder::default().on_client(client);

        Ok(Self {
            provider: Arc::new(provider),
            options,
        })
    }

    pub async fn get_block_header(
        &self,
        block_id: BlockId,
    ) -> Result<models::BlockWithTxHashes, JsonRpcProviderError> {
        let request = (|| async {
            match block_id {
                BlockId::Number(number) => {
                    self.provider
                        .client()
                        .request::<_, Option<models::BlockWithTxHashes>>(
                            "eth_getBlockByNumber",
                            (number, false),
                        )
                        .await
                }
                BlockId::Hash(hash) => {
                    let hash = BlockHash::from(hash);
                    self.provider
                        .client()
                        .request::<_, Option<models::BlockWithTxHashes>>(
                            "eth_getBlockByHash",
                            (hash, false),
                        )
                        .await
                }
            }
        })
        .retry(self.options.exponential_backoff);

        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(JsonRpcProviderError::Timeout)
                .attach_printable("failed to get block header")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .change_context(JsonRpcProviderError::Request)?
            .ok_or(JsonRpcProviderError::NotFound.into())
    }

    pub async fn get_block_with_transactions(
        &self,
        block_id: BlockId,
    ) -> Result<models::Block, JsonRpcProviderError> {
        let request = (|| async { self.provider.get_block(block_id).full().await })
            .retry(self.options.exponential_backoff);

        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(JsonRpcProviderError::Timeout)
                .attach_printable("failed to get block with transactions")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .change_context(JsonRpcProviderError::Request)?
            .ok_or(JsonRpcProviderError::NotFound.into())
    }

    pub async fn get_block_receipts(
        &self,
        block_id: BlockId,
    ) -> Result<Vec<models::TransactionReceipt>, JsonRpcProviderError> {
        let request = (|| async { self.provider.get_block_receipts(block_id).await })
            .retry(self.options.exponential_backoff);

        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(JsonRpcProviderError::Timeout)
                .attach_printable("failed to get block with receipts")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .change_context(JsonRpcProviderError::Request)?
            .ok_or(JsonRpcProviderError::NotFound.into())
    }

    pub async fn trace_block_transactions(
        &self,
        block_id: BlockId,
    ) -> Result<Vec<models::TraceResultsWithTransactionHash>, JsonRpcProviderError> {
        let request = (|| async {
            self.provider
                .trace_replay_block_transactions(block_id)
                .trace()
                .await
        })
        .retry(self.options.exponential_backoff);

        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(JsonRpcProviderError::Timeout)
                .attach_printable("failed to get block transaction traces")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response.change_context(JsonRpcProviderError::Request)
    }
}

impl error_stack::Context for JsonRpcProviderError {}

impl std::fmt::Display for JsonRpcProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonRpcProviderError::Request => write!(f, "failed to send request"),
            JsonRpcProviderError::Timeout => write!(f, "request timed out"),
            JsonRpcProviderError::NotFound => write!(f, "not found"),
            JsonRpcProviderError::Configuration => write!(f, "configuration error"),
        }
    }
}

impl JsonRpcProviderErrorExt for Report<JsonRpcProviderError> {
    fn is_not_found(&self) -> bool {
        matches!(self.current_context(), JsonRpcProviderError::NotFound)
    }
}
