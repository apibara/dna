use std::{fmt::Debug, num::NonZeroU32};

use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use ethers::prelude::*;
use futures_util::{future::join_all, Future};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, trace};

const DEFAULT_RATE_LIMIT: u32 = 1000;
const DEFAULT_CONCURRENCY: usize = 100;

pub mod models {
    pub use ethers::types::{
        transaction::eip2930::AccessListItem, Block, Bloom, Log, Transaction, TransactionReceipt,
        Withdrawal, H160, H256, H64, U128, U256,
    };
}

pub struct RpcProviderService {
    provider: Provider<Http>,
    rate_limit: u32,
    concurrency: usize,
}

pub struct RpcProvider {
    tx: async_channel::Sender<RpcServiceRequest>,
}

enum RpcServiceRequest {
    FinalizedBlock {
        reply: oneshot::Sender<Result<models::Block<models::H256>>>,
    },
    BlockByNumber {
        block_number: u64,
        reply: oneshot::Sender<Result<models::Block<models::H256>>>,
    },
    TransactionByHash {
        hash: models::H256,
        reply: oneshot::Sender<Result<models::Transaction>>,
    },
    TransactionReceiptByHash {
        hash: models::H256,
        reply: oneshot::Sender<Result<models::TransactionReceipt>>,
    },
}

impl RpcProviderService {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        let provider = Provider::<Http>::try_from(url.as_ref())
            .change_context(DnaError::Configuration)
            .attach_printable("failed to parse rpc provider url")
            .attach_printable_lazy(|| format!("url: {}", url.as_ref()))?;

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
                let tasks = (0..self.concurrency).into_iter().map(|wi| {
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
    pub async fn get_finalized_block(&self) -> Result<models::Block<models::H256>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::FinalizedBlock { reply: tx })
            .await
            .unwrap();
        let result = rx.await.change_context(DnaError::Fatal)?;
        result
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<models::Block<models::H256>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::BlockByNumber {
                block_number,
                reply: tx,
            })
            .await
            .unwrap();
        let result = rx.await.change_context(DnaError::Fatal)?;
        result
    }

    pub async fn get_transactions_by_hash(
        &self,
        hashes: &[models::H256],
    ) -> Result<Vec<models::Transaction>> {
        let futures = hashes
            .iter()
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

    pub async fn get_receipts_by_hash(
        &self,
        hashes: &[models::H256],
    ) -> Result<Vec<models::TransactionReceipt>> {
        let futures = hashes
            .iter()
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
    provider: Provider<Http>,
    limiter: DefaultDirectRateLimiter,
}

impl RpcWorker {
    pub fn new(worker_index: usize, provider: Provider<Http>, rate_limit: u32) -> Self {
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
            BlockByNumber {
                block_number,
                reply,
            } => {
                let response = self.get_block_by_number(block_number).await;
                let _ = reply.send(response);
            }
            TransactionByHash { hash, reply } => {
                let response = self.get_transaction_by_hash(hash).await;
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

    #[instrument(skip(self), err(Debug))]
    pub async fn get_transaction_by_hash(&self, hash: models::H256) -> Result<models::Transaction> {
        self.limiter.until_ready().await;

        let transaction = self
            .provider
            .get_transaction(hash)
            .await
            .change_context(DnaError::Fatal)?
            .ok_or(DnaError::Fatal)
            .attach_printable_lazy(|| format!("transaction not found: {:?}", hash))?;

        Ok(transaction)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_transaction_receipt_by_hash(
        &self,
        hash: models::H256,
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
        match self {
            RpcServiceRequest::FinalizedBlock { .. } => "FinalizedBlock",
            RpcServiceRequest::BlockByNumber { .. } => "BlockByNumber",
            RpcServiceRequest::TransactionByHash { .. } => "TransactionByHash",
            RpcServiceRequest::TransactionReceiptByHash { .. } => "TransactionReceiptByHash",
        }
    }
}

impl Debug for RpcServiceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcServiceRequest::FinalizedBlock { .. } => f.debug_struct("FinalizedBlock").finish(),
            RpcServiceRequest::BlockByNumber { block_number, .. } => f
                .debug_struct("BlockByNumber")
                .field("block_number", block_number)
                .finish(),
            RpcServiceRequest::TransactionByHash { hash, .. } => f
                .debug_struct("TransactionByHash")
                .field("hash", hash)
                .finish(),
            RpcServiceRequest::TransactionReceiptByHash { hash, .. } => f
                .debug_struct("TransactionReceiptByHash")
                .field("hash", hash)
                .finish(),
        }
    }
}
