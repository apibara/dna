use std::{fmt::Debug, num::NonZeroU32, sync::Arc};

use alloy_providers::provider::{Provider, TempProvider};
use alloy_rpc_client::RpcClient;
use alloy_transport::BoxTransport;
use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use futures_util::{future::join_all, Future, StreamExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, trace};
use url::Url;

const DEFAULT_RATE_LIMIT: u32 = 1000;
const DEFAULT_CONCURRENCY: usize = 100;

pub mod models {
    pub use alloy_primitives::{
        aliases::{BlockHash, BlockNumber, TxHash, B256, B64, U128, U256, U64, U8},
        Address, Bloom, FixedBytes,
    };
    pub use alloy_rpc_types::{
        AccessListItem, Block, BlockId, BlockNumberOrTag, BlockTransactions, Log, Transaction,
        TransactionReceipt, Withdrawal,
    };
}

pub struct RpcProviderService {
    provider: Arc<Provider<BoxTransport>>,
    rate_limit: u32,
    concurrency: usize,
}

pub struct RpcProvider {
    tx: async_channel::Sender<RpcServiceRequest>,
}

enum RpcServiceRequest {
    FinalizedBlock {
        reply: oneshot::Sender<Result<models::Block>>,
    },
    LatestBlock {
        reply: oneshot::Sender<Result<models::Block>>,
    },
    BlockByNumber {
        block_number: models::BlockNumber,
        reply: oneshot::Sender<Result<models::Block>>,
    },
    BlockByNumberWithTransactions {
        block_number: models::BlockNumber,
        reply: oneshot::Sender<Result<models::Block>>,
    },
    TransactionByHash {
        hash: models::TxHash,
        reply: oneshot::Sender<Result<models::Transaction>>,
    },
    BlockReceiptsByNumber {
        block_number: models::BlockNumber,
        reply: oneshot::Sender<Result<Vec<models::TransactionReceipt>>>,
    },
    TransactionReceiptByHash {
        hash: models::TxHash,
        reply: oneshot::Sender<Result<models::TransactionReceipt>>,
    },
}

impl RpcProviderService {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        let url: Url = Url::parse(url.as_ref())
            .change_context(DnaError::Configuration)
            .attach_printable_lazy(|| format!("failed to parse  provider url: {}", url.as_ref()))?;
        let client = RpcClient::builder().hyper_http(url).boxed();
        let provider = Provider::new_with_client(client).into();

        let concurrency = DEFAULT_CONCURRENCY;
        Ok(Self {
            provider,
            rate_limit: DEFAULT_RATE_LIMIT,
            concurrency,
        })
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_rate_limit(mut self, rate_limit: u32) -> Self {
        self.rate_limit = rate_limit;
        self
    }

    pub fn start(self, ct: CancellationToken) -> (RpcProvider, impl Future<Output = Result<()>>) {
        let (tx, rx) = async_channel::unbounded();
        let worker_rate_limit = self.rate_limit / self.concurrency as u32;

        info!(
            rate_limit = worker_rate_limit,
            concurrency = self.concurrency,
            "starting rpc provider service"
        );

        let fut = {
            async move {
                let tasks = (0..self.concurrency).map(|wi| {
                    let ct = ct.clone();
                    let worker = RpcWorker::new(wi, self.provider.clone(), worker_rate_limit);
                    tokio::task::spawn(worker.start(rx.clone(), ct))
                });

                join_all(tasks).await;
                Ok(())
            }
        };

        let provider = RpcProvider { tx };
        (provider, fut)
    }
}

impl RpcProvider {
    pub async fn get_finalized_block(&self) -> Result<models::Block> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::FinalizedBlock { reply: tx })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    pub async fn get_latest_block(&self) -> Result<models::Block> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::LatestBlock { reply: tx })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number(
        &self,
        block_number: models::BlockNumber,
    ) -> Result<models::Block> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::BlockByNumber {
                block_number,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number_with_transactions(
        &self,
        block_number: models::BlockNumber,
    ) -> Result<models::Block> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::BlockByNumberWithTransactions {
                block_number,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    pub async fn get_transactions_by_hash(
        &self,
        hashes: impl Iterator<Item = &models::TxHash>,
    ) -> Result<Vec<models::Transaction>> {
        let futures = hashes
            .map(|hash| async {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .tx
                    .send(RpcServiceRequest::TransactionByHash {
                        hash: *hash,
                        reply: tx,
                    })
                    .await;
                rx.await.change_context(DnaError::Fatal)
            })
            .collect::<Vec<_>>();

        let transactions = join_all(futures)
            .await
            .into_iter()
            .map(|result| result.change_context(DnaError::Fatal)?)
            .collect::<Result<Vec<_>>>()?;

        Ok(transactions)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_receipts_by_number(
        &self,
        block_number: u64,
    ) -> Result<Vec<models::TransactionReceipt>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::BlockReceiptsByNumber {
                block_number,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    pub async fn get_receipts_by_hash(
        &self,
        hashes: impl Iterator<Item = &models::TxHash>,
    ) -> Result<Vec<models::TransactionReceipt>> {
        let futures = hashes
            .map(|hash| async {
                let (tx, rx) = oneshot::channel();
                let _ = self
                    .tx
                    .send(RpcServiceRequest::TransactionReceiptByHash {
                        hash: *hash,
                        reply: tx,
                    })
                    .await;
                rx.await.change_context(DnaError::Fatal)
            })
            .collect::<Vec<_>>();

        let receipts = join_all(futures)
            .await
            .into_iter()
            .map(|result| result.change_context(DnaError::Fatal)?)
            .collect::<Result<Vec<_>>>()?;

        Ok(receipts)
    }
}

struct RpcWorker {
    worker_index: usize,
    provider: Arc<Provider<BoxTransport>>,
    limiter: DefaultDirectRateLimiter,
}

impl RpcWorker {
    pub fn new(
        worker_index: usize,
        provider: Arc<Provider<BoxTransport>>,
        rate_limit: u32,
    ) -> Self {
        let limiter = new_limiter(rate_limit);

        Self {
            worker_index,
            provider,
            limiter,
        }
    }

    pub async fn start(
        self,
        rx: async_channel::Receiver<RpcServiceRequest>,
        ct: CancellationToken,
    ) -> Result<()> {
        trace!(index = self.worker_index, "starting rpc worker");
        tokio::pin!(rx);
        loop {
            tokio::select! {
                msg = rx.next() => {
                    match msg {
                        None => break,
                        Some(request) => self.handle_request(request).await?,
                    }
                },
                _ = ct.cancelled() => {
                    break;
                }
            }
        }

        trace!(index = self.worker_index, "rpc worker finished");

        Ok(())
    }

    #[instrument(skip_all, fields(tag = %request.tag()), err(Debug))]
    async fn handle_request(&self, request: RpcServiceRequest) -> Result<()> {
        use RpcServiceRequest::*;
        trace!(index = self.worker_index, request = ?request, "handling request");
        match request {
            FinalizedBlock { reply } => {
                let response = self.get_finalized_block().await;
                let _ = reply.send(response);
            }
            LatestBlock { reply } => {
                let response = self.get_latest_block().await;
                let _ = reply.send(response);
            }
            BlockByNumber {
                block_number,
                reply,
            } => {
                let response = self.get_block_by_number(block_number).await;
                let _ = reply.send(response);
            }
            BlockByNumberWithTransactions {
                block_number,
                reply,
            } => {
                let response = self
                    .get_block_by_number_with_transactions(block_number)
                    .await;
                let _ = reply.send(response);
            }
            TransactionByHash { hash, reply } => {
                let response = self.get_transaction_by_hash(hash).await;
                let _ = reply.send(response);
            }
            BlockReceiptsByNumber {
                block_number,
                reply,
            } => {
                let response = self.get_block_receipts_by_number(block_number).await;
                let _ = reply.send(response);
            }
            TransactionReceiptByHash { hash, reply } => {
                let response = self.get_transaction_receipt_by_hash(hash).await;
                let _ = reply.send(response);
            }
        }

        Ok(())
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_finalized_block(&self) -> Result<models::Block> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_by_number(models::BlockNumberOrTag::Finalized, false)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get finalized block")?
            .ok_or(DnaError::Fatal)
            .attach_printable("block not found")?;

        Ok(block)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_latest_block(&self) -> Result<models::Block> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_by_number(models::BlockNumberOrTag::Latest, false)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get latest block")?
            .ok_or(DnaError::Fatal)
            .attach_printable("block not found")?;

        Ok(block)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number(
        &self,
        block_number: models::BlockNumber,
    ) -> Result<models::Block> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_by_number(block_number.into(), false)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to get block: {}", block_number))?
            .ok_or(DnaError::Fatal)
            .attach_printable_lazy(|| format!("block not found: {:?}", block_number))?;

        Ok(block)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number_with_transactions(
        &self,
        block_number: models::BlockNumber,
    ) -> Result<models::Block> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_by_number(block_number.into(), true)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable_lazy(|| format!("failed to get block: {}", block_number))?
            .ok_or(DnaError::Fatal)
            .attach_printable_lazy(|| format!("block not found: {:?}", block_number))?;

        Ok(block)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_transaction_by_hash(
        &self,
        hash: models::TxHash,
    ) -> Result<models::Transaction> {
        self.limiter.until_ready().await;

        let transaction = self
            .provider
            .get_transaction_by_hash(hash)
            .await
            .change_context(DnaError::Fatal)?;

        Ok(transaction)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_receipts_by_number(
        &self,
        block_number: models::BlockNumber,
    ) -> Result<Vec<models::TransactionReceipt>> {
        self.limiter.until_ready().await;

        let receipts = self
            .provider
            .get_block_receipts(block_number.into())
            .await
            .change_context(DnaError::Fatal)?
            .ok_or(DnaError::Fatal)?;

        Ok(receipts)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_transaction_receipt_by_hash(
        &self,
        hash: models::TxHash,
    ) -> Result<models::TransactionReceipt> {
        self.limiter.until_ready().await;

        let receipt = self
            .provider
            .get_transaction_receipt(hash)
            .await
            .change_context(DnaError::Fatal)?
            .ok_or(DnaError::Fatal)
            .attach_printable_lazy(|| format!("transaction receipt not found: {:?}", hash))?;

        Ok(receipt)
    }
}

fn new_limiter(rate_limit: u32) -> DefaultDirectRateLimiter {
    let quota_per_second = NonZeroU32::new(1 + rate_limit).unwrap();
    let quota = Quota::per_second(quota_per_second).allow_burst(quota_per_second);
    RateLimiter::direct(quota)
}

impl RpcServiceRequest {
    pub fn tag(&self) -> &'static str {
        use RpcServiceRequest::*;
        match self {
            FinalizedBlock { .. } => "FinalizedBlock",
            LatestBlock { .. } => "LatestBlock",
            BlockByNumber { .. } => "BlockByNumber",
            BlockByNumberWithTransactions { .. } => "BlockByNumberWithTransactions",
            TransactionByHash { .. } => "TransactionByHash",
            BlockReceiptsByNumber { .. } => "BlockReceiptsByNumber",
            TransactionReceiptByHash { .. } => "TransactionReceiptByHash",
        }
    }
}

impl Debug for RpcServiceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use RpcServiceRequest::*;
        match self {
            FinalizedBlock { .. } => f.debug_struct("FinalizedBlock").finish(),
            LatestBlock { .. } => f.debug_struct("LatestBlock").finish(),
            BlockByNumber { block_number, .. } => f
                .debug_struct("BlockByNumber")
                .field("block_number", block_number)
                .finish(),
            BlockByNumberWithTransactions { block_number, .. } => f
                .debug_struct("BlockByNumberWithTransactions")
                .field("block_number", block_number)
                .finish(),
            TransactionByHash { hash, .. } => f
                .debug_struct("TransactionByHash")
                .field("hash", hash)
                .finish(),
            BlockReceiptsByNumber { block_number, .. } => f
                .debug_struct("BlockReceiptsByNumber")
                .field("block_number", block_number)
                .finish(),
            TransactionReceiptByHash { hash, .. } => f
                .debug_struct("TransactionReceiptByHash")
                .field("hash", hash)
                .finish(),
        }
    }
}
