use std::time::Duration;

use alloy_provider::{network::Ethereum, Provider, ProviderBuilder};
use alloy_rpc_client::ClientBuilder;
use alloy_transport::BoxTransport;
use error_stack::{Result, ResultExt};
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
}

pub struct JsonRpcProvider {
    provider: Box<dyn Provider<BoxTransport, Ethereum>>,
    options: JsonRpcProviderOptions,
}

impl JsonRpcProvider {
    pub fn new(url: Url, options: JsonRpcProviderOptions) -> Result<Self, JsonRpcProviderError> {
        if !options.headers.is_empty() {
            return Err(JsonRpcProviderError::Configuration)
                .attach_printable("custom headers are not supported");
        }

        let client = ClientBuilder::default().http(url);
        let provider = ProviderBuilder::default().on_client(client).boxed();

        Ok(Self {
            provider: Box::new(provider),
            options,
        })
    }

    pub async fn get_block_with_transactions(
        &self,
        block_id: BlockId,
    ) -> Result<models::Block, JsonRpcProviderError> {
        let request = self
            .provider
            .get_block(block_id, alloy_rpc_types::BlockTransactionsKind::Full);

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
        let request = self.provider.get_block_receipts(block_id);

        let Ok(response) = tokio::time::timeout(self.options.timeout, request).await else {
            return Err(JsonRpcProviderError::Timeout)
                .attach_printable("failed to get block with receipts")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"));
        };

        response
            .change_context(JsonRpcProviderError::Request)?
            .ok_or(JsonRpcProviderError::NotFound.into())
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
