use std::{num::NonZeroU32, sync::Arc};

use alloy_provider::{network::Ethereum, Provider, ProviderBuilder};
use alloy_rpc_types::BlockTransactionsKind;
use alloy_transport::BoxTransport;
use error_stack::{Result, ResultExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::Semaphore;
use url::Url;

pub mod models {
    use apibara_dna_common::core::Cursor;

    pub use alloy_primitives::{
        aliases::{BlockHash, BlockNumber, TxHash, B256, B64, U128, U256, U64, U8},
        Address, Bloom, FixedBytes,
    };
    pub use alloy_rpc_types::{
        AccessListItem, Block, BlockId, BlockNumberOrTag, BlockTransactions, Header, Log,
        Transaction, TransactionReceipt, Withdrawal,
    };

    pub trait BlockExt {
        fn cursor(&self) -> Option<Cursor>;
    }

    impl BlockExt for Block {
        fn cursor(&self) -> Option<Cursor> {
            let number = self.header.number?;
            let hash = self.header.hash?;
            Cursor::new(number, hash.to_vec()).into()
        }
    }

    pub trait BlockIdExt {
        fn to_block_id(&self) -> BlockId;
    }

    impl BlockIdExt for Cursor {
        fn to_block_id(&self) -> BlockId {
            if self.hash.is_empty() {
                return BlockId::Number(BlockNumberOrTag::Number(self.number));
            }
            let hash = B256::from_slice(&self.hash);
            BlockId::Hash(hash.into())
        }
    }
}

pub struct JsonRpcProviderFactory {
    url: Url,
    options: JsonRpcProviderOptions,
    semaphore: Arc<Semaphore>,
}

pub struct JsonRpcProvider {
    provider: Box<dyn Provider<BoxTransport, Ethereum>>,
    limiter: DefaultDirectRateLimiter,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct JsonRpcProviderOptions {
    pub rate_limit: u32,
}

#[derive(Debug)]
pub struct JsonRpcProviderError;

impl JsonRpcProviderFactory {
    pub fn new(url: Url, options: JsonRpcProviderOptions, semaphore: Arc<Semaphore>) -> Self {
        Self {
            url,
            options,
            semaphore,
        }
    }

    pub fn new_provider(&self) -> JsonRpcProvider {
        JsonRpcProvider::new(
            self.url.clone(),
            self.semaphore.clone(),
            self.options.clone(),
        )
    }
}

impl JsonRpcProvider {
    pub fn new(url: Url, semaphore: Arc<Semaphore>, options: JsonRpcProviderOptions) -> Self {
        let provider = ProviderBuilder::default().on_http(url).boxed();
        let limiter = new_limiter(options.rate_limit);
        Self {
            provider: Box::new(provider),
            limiter,
            semaphore,
        }
    }

    pub async fn get_block(
        &self,
        id: models::BlockId,
    ) -> Result<Option<models::Block>, JsonRpcProviderError> {
        let _semaphore = self
            .semaphore
            .acquire()
            .await
            .change_context(JsonRpcProviderError);
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block(id, BlockTransactionsKind::Hashes)
            .await
            .change_context(JsonRpcProviderError)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("block id: {:?}", id))?;
        Ok(block)
    }
}

fn new_limiter(rate_limit: u32) -> DefaultDirectRateLimiter {
    let quota = NonZeroU32::new(u32::max(rate_limit, 1)).expect("rate limit must be positive");
    let quota = Quota::per_second(quota).allow_burst(quota);
    RateLimiter::direct(quota)
}

impl Default for JsonRpcProviderOptions {
    fn default() -> Self {
        Self { rate_limit: 1_000 }
    }
}

impl error_stack::Context for JsonRpcProviderError {}

impl std::fmt::Display for JsonRpcProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON RPC provider error")
    }
}
