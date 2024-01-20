use std::num::NonZeroU32;

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use futures_util::{stream, StreamExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use starknet::providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider, Url};
use tracing::instrument;

pub mod models {
    pub use starknet::core::types::*;
}

pub struct RpcProvider {
    inner: JsonRpcClient<HttpTransport>,
    limiter: DefaultDirectRateLimiter,
    concurrency: usize,
}

impl RpcProvider {
    pub fn new(url: Url, rate_limit: u32) -> Self {
        let transport = HttpTransport::new(url);
        let inner = JsonRpcClient::new(transport);

        let quota_per_second = NonZeroU32::new(1 + rate_limit).unwrap();
        let quota = Quota::per_second(quota_per_second).allow_burst(quota_per_second);
        let limiter = RateLimiter::direct(quota);

        Self {
            inner,
            limiter,
            concurrency: 20,
        }
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_with_tx_hashes(
        &self,
        block_id: &models::BlockId,
    ) -> Result<models::MaybePendingBlockWithTxHashes> {
        self.limiter.until_ready().await;

        let response = self
            .inner
            .get_block_with_tx_hashes(block_id)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| {
                format!("failed to get block with tx hashes: {block_id:?}")
            })?;

        Ok(response)
    }

    pub async fn get_transactions_by_hash(
        &self,
        hashes: &[models::FieldElement],
    ) -> Result<Vec<models::Transaction>> {
        let transactions = stream::iter(hashes)
            .map(|tx_hash| async move { self.get_transaction_by_hash(tx_hash).await })
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(transactions)
    }

    pub async fn get_transactions_receipts(
        &self,
        hashes: &[models::FieldElement],
    ) -> Result<Vec<models::TransactionReceipt>> {
        let receipts = stream::iter(hashes)
            .map(|tx_hash| async move { self.get_transaction_receipt(tx_hash).await })
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(receipts)
    }

    pub async fn get_transaction_by_hash(
        &self,
        hash: &models::FieldElement,
    ) -> Result<models::Transaction> {
        self.limiter.until_ready().await;

        let response = self
            .inner
            .get_transaction_by_hash(hash)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to get tx by hash: {hash:?}"))?;

        Ok(response)
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: &models::FieldElement,
    ) -> Result<models::TransactionReceipt> {
        use models::MaybePendingTransactionReceipt::*;

        self.limiter.until_ready().await;

        let response = self
            .inner
            .get_transaction_receipt(hash)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to get tx by hash: {hash:?}"))?;

        match response {
            Receipt(receipt) => Ok(receipt),
            PendingReceipt(_) => {
                Err(DnaError::Fatal).attach_printable("expected receipt, got pending receipt")
            }
        }
    }
}
