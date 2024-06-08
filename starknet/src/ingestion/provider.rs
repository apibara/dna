use std::{fmt::Debug, num::NonZeroU32, sync::Arc};

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
};
use error_stack::ResultExt;
use futures_util::{future::join_all, StreamExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use starknet::{
    core::types::{BlockId, BlockTag},
    providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider, Url},
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, trace};

use crate::segment::conversion::{BlockExt, GetCursor};

const DEFAULT_RATE_LIMIT: u32 = 1000;
const DEFAULT_CONCURRENCY: usize = 100;

pub mod models {
    pub use starknet::core::types::{
        BlockStatus, BlockWithReceipts, BlockWithTxHashes, BlockWithTxs, ComputationResources,
        DataAvailabilityMode, DataResources, DeclareTransaction, DeclareTransactionReceipt,
        DeclareTransactionV0, DeclareTransactionV1, DeclareTransactionV2, DeclareTransactionV3,
        DeployAccountTransaction, DeployAccountTransactionReceipt, DeployAccountTransactionV1,
        DeployAccountTransactionV3, DeployTransaction, DeployTransactionReceipt, Event,
        ExecutionResources, ExecutionResult, FeePayment, FieldElement, InvokeTransaction,
        InvokeTransactionReceipt, InvokeTransactionV0, InvokeTransactionV1, InvokeTransactionV3,
        L1DataAvailabilityMode, L1HandlerTransaction, L1HandlerTransactionReceipt,
        MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes, MaybePendingBlockWithTxs,
        MsgToL1, PendingBlockWithTxHashes, PriceUnit, ResourceBounds, ResourceBoundsMapping,
        ResourcePrice, Transaction, TransactionReceipt,
    };
}

pub struct RpcProviderService {
    provider: Arc<JsonRpcClient<HttpTransport>>,
    rate_limit: u32,
    concurrency: usize,
}

#[derive(Clone)]
pub struct RpcProvider {
    tx: async_channel::Sender<RpcServiceRequest>,
}

enum RpcServiceRequest {
    FinalizedBlock {
        latest_hint: Option<u64>,
        finalized_hint: Option<u64>,
        reply: oneshot::Sender<Result<Cursor>>,
    },
    LatestBlock {
        reply: oneshot::Sender<Result<Cursor>>,
    },
    BlockByNumberWithReceipts {
        block_number: u64,
        reply: oneshot::Sender<Result<models::BlockWithReceipts>>,
    },
}

impl RpcProviderService {
    pub fn new(url: impl AsRef<str>) -> Result<Self> {
        let url: Url = Url::parse(url.as_ref())
            .change_context(DnaError::Configuration)
            .attach_printable_lazy(|| format!("failed to parse provider url: {}", url.as_ref()))?;

        let transport = HttpTransport::new(url);
        let provider = JsonRpcClient::new(transport);

        Ok(Self {
            provider: provider.into(),
            rate_limit: DEFAULT_RATE_LIMIT,
            concurrency: DEFAULT_CONCURRENCY,
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

    pub fn start(self, ct: CancellationToken) -> RpcProvider {
        let (tx, rx) = async_channel::unbounded();

        let worker_rate_limit = self.rate_limit / self.concurrency as u32;

        info!(
            rate_limit = worker_rate_limit,
            concurrency = self.concurrency,
            "starting rpc provider service"
        );

        tokio::spawn(async move {
            let tasks = (0..self.concurrency).map(|wi| {
                let ct = ct.clone();
                let worker = RpcWorker::new(wi, self.provider.clone(), worker_rate_limit);
                tokio::task::spawn(worker.start(rx.clone(), ct))
            });

            join_all(tasks).await;
        });

        RpcProvider { tx }
    }
}

impl RpcProvider {
    pub async fn get_latest_block(&self) -> Result<Cursor> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::LatestBlock { reply: tx })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    pub async fn get_finalized_block(
        &self,
        finalized_hint: Option<u64>,
        latest_hint: Option<u64>,
    ) -> Result<Cursor> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::FinalizedBlock {
                finalized_hint,
                latest_hint,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }

    pub async fn get_block_by_number_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<models::BlockWithReceipts> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RpcServiceRequest::BlockByNumberWithReceipts {
                block_number,
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.change_context(DnaError::Fatal)?
    }
}

struct RpcWorker {
    worker_index: usize,
    provider: Arc<JsonRpcClient<HttpTransport>>,
    limiter: DefaultDirectRateLimiter,
}

impl RpcWorker {
    pub fn new(
        worker_index: usize,
        provider: Arc<JsonRpcClient<HttpTransport>>,
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
            FinalizedBlock {
                finalized_hint,
                latest_hint,
                reply,
            } => {
                let response = self.get_finalized_block(finalized_hint, latest_hint).await;
                let _ = reply.send(response);
            }
            LatestBlock { reply } => {
                let response = self.get_latest_block().await;
                let _ = reply.send(response);
            }
            BlockByNumberWithReceipts {
                block_number,
                reply,
            } => {
                let response = self.get_block_by_number_with_receipts(block_number).await;
                let _ = reply.send(response);
            }
        }

        Ok(())
    }

    #[instrument(skip_all, err(Debug))]
    pub async fn get_finalized_block(
        &self,
        finalized_hint: Option<u64>,
        latest_hint: Option<u64>,
    ) -> Result<Cursor> {
        self.limiter.until_ready().await;

        // Starknet doesn't provide a quick way to get the finalized block.
        // So we need to perform a binary search to find it.
        let mut accepted_number = if let Some(hint) = latest_hint {
            hint
        } else {
            let cursor = self
                .provider
                .get_block_with_tx_hashes(BlockId::Tag(BlockTag::Latest))
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to get latest block")?
                .cursor()
                .ok_or(DnaError::Fatal)
                .attach_printable("latest block is missing cursor")?;
            cursor.number
        };

        let finalized_number = if let Some(hint) = finalized_hint {
            hint
        } else {
            // Just find a block that's finalized.
            let mut number = accepted_number;
            loop {
                number -= 100;
                let block = self
                    .provider
                    .get_block_with_tx_hashes(BlockId::Number(number))
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to get block")?;

                if block.is_finalized() {
                    break;
                }
            }

            number
        };

        let mut finalized = self
            .provider
            .get_block_with_tx_hashes(BlockId::Number(finalized_number))
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get block by number")?
            .cursor()
            .ok_or(DnaError::Fatal)
            .attach_printable("block is missing cursor")?;

        let mut step_count = 0;
        loop {
            step_count += 1;

            if step_count > 200 {
                return Err(DnaError::Fatal)
                    .attach_printable("too many steps in finalized block binary search");
            }

            let mid_block_number = finalized.number + (accepted_number - finalized.number) / 2;

            if mid_block_number <= finalized.number {
                break;
            }

            let mid_block = self
                .provider
                .get_block_with_tx_hashes(BlockId::Number(mid_block_number))
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to get block by number")?;

            let mid_cursor = mid_block
                .cursor()
                .ok_or(DnaError::Fatal)
                .attach_printable("block is missing cursor")?;

            if mid_block.is_finalized() {
                finalized = mid_cursor;
            } else {
                accepted_number = mid_cursor.number;
            }
        }

        Ok(finalized)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_latest_block(&self) -> Result<Cursor> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_with_tx_hashes(BlockId::Tag(BlockTag::Latest))
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get latest block")?;

        let cursor = block
            .cursor()
            .ok_or(DnaError::Fatal)
            .attach_printable("latest block is missing cursor")?;

        Ok(cursor)
    }

    #[instrument(skip(self), err(Debug))]
    pub async fn get_block_by_number_with_receipts(
        &self,
        block_number: u64,
    ) -> Result<models::BlockWithReceipts> {
        self.limiter.until_ready().await;

        let block = self
            .provider
            .get_block_with_receipts(BlockId::Number(block_number))
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to get block by number")?;

        let models::MaybePendingBlockWithReceipts::Block(block) = block else {
            return Err(DnaError::Fatal)
                .attach_printable("expected block with receipts, got pending block");
        };

        Ok(block)
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
            BlockByNumberWithReceipts { .. } => "BlockByNumberWithReceipts",
        }
    }
}

impl Debug for RpcServiceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use RpcServiceRequest::*;
        match self {
            FinalizedBlock { .. } => f.debug_struct("FinalizedBlock").finish(),
            LatestBlock { .. } => f.debug_struct("LatestBlock").finish(),
            BlockByNumberWithReceipts { .. } => {
                f.debug_struct("BlockByNumberWithReceipts").finish()
            }
        }
    }
}
