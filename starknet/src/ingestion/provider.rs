use std::{fmt::Debug, num::NonZeroU32, sync::Arc};

use error_stack::{Result, ResultExt};
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use starknet::{
    core::types::BlockId,
    providers::{jsonrpc::HttpTransport, JsonRpcClient, Provider, Url},
};
use tokio::sync::Semaphore;

pub mod models {
    use apibara_dna_common::core::Cursor;
    pub use starknet::core::types::{
        BlockId, BlockStatus, BlockTag, BlockWithReceipts, BlockWithTxHashes, BlockWithTxs,
        ComputationResources, DataAvailabilityMode, DataResources, DeclareTransaction,
        DeclareTransactionReceipt, DeclareTransactionV0, DeclareTransactionV1,
        DeclareTransactionV2, DeclareTransactionV3, DeployAccountTransaction,
        DeployAccountTransactionReceipt, DeployAccountTransactionV1, DeployAccountTransactionV3,
        DeployTransaction, DeployTransactionReceipt, Event, ExecutionResources, ExecutionResult,
        FeePayment, FieldElement, InvokeTransaction, InvokeTransactionReceipt, InvokeTransactionV0,
        InvokeTransactionV1, InvokeTransactionV3, L1DataAvailabilityMode, L1HandlerTransaction,
        L1HandlerTransactionReceipt, MaybePendingBlockWithReceipts, MaybePendingBlockWithTxHashes,
        MaybePendingBlockWithTxs, MsgToL1, PendingBlockWithTxHashes, PriceUnit, ResourceBounds,
        ResourceBoundsMapping, ResourcePrice, Transaction, TransactionReceipt,
    };

    pub trait BlockIdExt {
        fn to_block_id(&self) -> BlockId;
    }

    impl BlockIdExt for Cursor {
        fn to_block_id(&self) -> BlockId {
            if self.hash.is_empty() {
                return BlockId::Number(self.number);
            }
            let hash = FieldElement::from_byte_slice_be(&self.hash).expect("invalid hash");
            BlockId::Hash(hash)
        }
    }

    pub trait BlockExt {
        fn parent_hash(&self) -> &FieldElement;
        fn cursor(&self) -> Option<Cursor>;
        fn is_finalized(&self) -> bool;
    }

    impl BlockExt for MaybePendingBlockWithTxHashes {
        fn cursor(&self) -> Option<Cursor> {
            match self {
                MaybePendingBlockWithTxHashes::Block(block) => {
                    let number = block.block_number;
                    let hash = block.block_hash.to_bytes_be().to_vec();
                    Cursor::new(number, hash).into()
                }
                MaybePendingBlockWithTxHashes::PendingBlock(_) => None,
            }
        }

        fn parent_hash(&self) -> &FieldElement {
            match self {
                MaybePendingBlockWithTxHashes::Block(block) => &block.parent_hash,
                MaybePendingBlockWithTxHashes::PendingBlock(block) => &block.parent_hash,
            }
        }

        fn is_finalized(&self) -> bool {
            match self {
                MaybePendingBlockWithTxHashes::Block(block) => {
                    block.status == BlockStatus::AcceptedOnL1
                }
                MaybePendingBlockWithTxHashes::PendingBlock(_) => false,
            }
        }
    }
}

pub struct JsonRpcProviderFactory {
    url: Url,
    options: JsonRpcProviderOptions,
    semaphore: Arc<Semaphore>,
}

pub struct JsonRpcProvider {
    client: JsonRpcClient<HttpTransport>,
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
        let transport = HttpTransport::new(url);
        let client = JsonRpcClient::new(transport);
        let limiter = new_limiter(options.rate_limit);
        Self {
            client,
            limiter,
            semaphore,
        }
    }

    pub async fn get_block(
        &self,
        id: impl AsRef<BlockId> + Send + Sync + Debug,
    ) -> Result<models::MaybePendingBlockWithTxHashes, JsonRpcProviderError> {
        let _semaphore = self
            .semaphore
            .acquire()
            .await
            .change_context(JsonRpcProviderError);
        self.limiter.until_ready().await;

        let id = id.as_ref();

        let response = self
            .client
            .get_block_with_tx_hashes(id)
            .await
            .change_context(JsonRpcProviderError)
            .attach_printable("failed to get block")
            .attach_printable_lazy(|| format!("block id: {:?}", id))?;

        Ok(response)
    }

    pub async fn get_block_with_receipts(
        &self,
        id: impl AsRef<BlockId> + Send + Sync + Debug,
    ) -> Result<models::MaybePendingBlockWithReceipts, JsonRpcProviderError> {
        let _semaphore = self
            .semaphore
            .acquire()
            .await
            .change_context(JsonRpcProviderError);
        self.limiter.until_ready().await;

        let id = id.as_ref();

        let response = self
            .client
            .get_block_with_receipts(id)
            .await
            .change_context(JsonRpcProviderError)
            .attach_printable("failed to get block w/ receipts")
            .attach_printable_lazy(|| format!("block id: {:?}", id))?;

        Ok(response)
    }
}

fn new_limiter(rate_limit: u32) -> DefaultDirectRateLimiter {
    let quota = NonZeroU32::new(u32::max(rate_limit, 1)).expect("rate limit must be positive");
    let quota = Quota::per_second(quota).allow_burst(quota);
    RateLimiter::direct(quota)
}

impl error_stack::Context for JsonRpcProviderError {}

impl std::fmt::Display for JsonRpcProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Starknet RPC error")
    }
}
