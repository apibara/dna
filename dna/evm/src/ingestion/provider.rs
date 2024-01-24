use std::num::NonZeroU32;

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use ethers::prelude::*;
use futures_util::stream;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tracing::instrument;

const DEFAULT_RATE_LIMIT: u32 = 1000;
const DEFAULT_CONCURRENCY: usize = 100;

pub mod models {
    pub use ethers::types::{
        transaction::eip2930::AccessListItem, Block, Bloom, Log, Transaction, TransactionReceipt,
        Withdrawal, H160, H256, H64, U128, U256,
    };
}

pub struct RpcProvider {
    provider: Provider<Http>,
    limiter: DefaultDirectRateLimiter,
    concurrency: usize,
}

impl RpcProvider {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        let provider = Provider::<Http>::try_from(url.as_ref())
            .change_context(DnaError::Configuration)
            .attach_printable("failed to parse rpc provider url")
            .attach_printable_lazy(|| format!("url: {}", url.as_ref()))?;

        let limiter = new_limiter(DEFAULT_RATE_LIMIT);
        let concurrency = DEFAULT_CONCURRENCY;
        Ok(Self {
            provider,
            limiter,
            concurrency,
        })
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_rate_limit(mut self, rate_limit: u32) -> Self {
        self.limiter = new_limiter(rate_limit);
        self
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_finalized_block(&self) -> Result<models::Block<models::H256>> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block(BlockNumber::Finalized)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get finalized block")?
            .ok_or(DnaError::Fatal)
            .attach_printable("block not found")?;

        Ok(block)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<models::Block<models::H256>> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block(block_number)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to get block: {}", block_number))?
            .ok_or(DnaError::Fatal)
            .attach_printable_lazy(|| format!("block not found: {:?}", block_number))?;

        Ok(block)
    }

    pub async fn get_transactions_by_hash(
        &self,
        hashes: &[models::H256],
    ) -> Result<Vec<models::Transaction>> {
        let transactions = stream::iter(hashes)
            .map(|tx_hash| async move {
                self.limiter.until_ready().await;
                let tx = self
                    .provider
                    .get_transaction(*tx_hash)
                    .await
                    .change_context(DnaError::Fatal)?;
                tx.ok_or(DnaError::Fatal)
                    .attach_printable_lazy(|| format!("transaction not found: {:?}", tx_hash))
            })
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(transactions)
    }

    pub async fn get_receipts_by_hash(
        &self,
        hashes: &[models::H256],
    ) -> Result<Vec<models::TransactionReceipt>> {
        let receipts = stream::iter(hashes)
            .map(|tx_hash| async move {
                self.limiter.until_ready().await;
                let tx = self
                    .provider
                    .get_transaction_receipt(*tx_hash)
                    .await
                    .change_context(DnaError::Fatal)?;
                tx.ok_or(DnaError::Fatal)
                    .attach_printable_lazy(|| format!("transaction not found: {:?}", tx_hash))
            })
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(receipts)
    }
}

fn new_limiter(rate_limit: u32) -> DefaultDirectRateLimiter {
    let quota_per_second = NonZeroU32::new(1 + rate_limit).unwrap();
    let quota = Quota::per_second(quota_per_second).allow_burst(quota_per_second);
    RateLimiter::direct(quota)
}
