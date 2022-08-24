//! A task that tracks the current network's head

use std::{sync::Arc, time::Duration};

use apibara_node::{
    db::{
        libmdbx::{Environment, EnvironmentKind},
        tables::BlockHash,
        KeyDecodeError,
    },
    head_tracker::{BlockHeader, BlockId, HeadTracker, HeadTrackerError, HeadTrackerResponse},
};
use starknet::providers::jsonrpc::{
    models::{Block, BlockHashOrTag, BlockNumOrTag, BlockTag},
    HttpTransport, JsonRpcClient, JsonRpcClientError, JsonRpcTransport,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use url::Url;

use crate::core::StarkNetBlockHash;

#[derive(Debug, thiserror::Error)]
pub enum StarkNetChainTrackerError {
    #[error("unexpected pending block")]
    UnexpectedPendingBlock,
    #[error("error originating from the chain tracker state machine")]
    StateMachine(#[from] HeadTrackerError),
    #[error("error performing a json-rpc request")]
    Rpc(#[from] JsonRpcClientError<<HttpTransport as JsonRpcTransport>::Error>),
    #[error("error decoding hash")]
    HashDecodeError(#[from] KeyDecodeError),
    #[error("block has unexpected block number")]
    BlockNumberMismatch { expected: u64, actual: u64 },
}

pub type Result<T> = std::result::Result<T, StarkNetChainTrackerError>;

/// Track StarkNet heads.
pub struct StarkNetChainTracker<E: EnvironmentKind> {
    pub tracker: HeadTracker<StarkNetBlockHash, E>,
    pub provider: JsonRpcClient<HttpTransport>,
}

impl<E> StarkNetChainTracker<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, provider_url: Url) -> Result<Self> {
        let provider = JsonRpcClient::new(HttpTransport::new(provider_url));
        let tracker = HeadTracker::new(db, 20)?;
        Ok(StarkNetChainTracker { tracker, provider })
    }

    pub async fn start(self, ct: CancellationToken) -> Result<()> {
        info!("starting chain tracker");

        let short_wait_duration = Duration::from_secs(5);

        loop {
            if ct.is_cancelled() {
                break;
            }

            debug!("refreshing current head");
            let block = self
                .provider
                .get_block_by_number(&BlockNumOrTag::Tag(BlockTag::Latest))
                .await?;
            let block_header = to_block_header(&block)?;
            match self.tracker.apply_head(block_header)? {
                HeadTrackerResponse::AlreadySeenBlock => {
                    debug!("head already seen");
                    tokio::select! {
                        _ = tokio::time::sleep(short_wait_duration) => {},
                        _ = ct.cancelled() => {},
                    }
                }
                HeadTrackerResponse::BlockGap(missing_block) => {
                    self.handle_block_gap(&missing_block).await?;
                }
                HeadTrackerResponse::MissingBlockHeaders(missing) => {
                    self.handle_missing_block_headers(&missing).await?;
                }
                HeadTrackerResponse::NewHead(head) => {
                    info!(head = ?head, "new head");
                }
                HeadTrackerResponse::Reorg(_) => todo!(),
            }
        }
        info!("shutting down chain tracker");
        Ok(())
    }

    async fn handle_missing_block_headers(
        &self,
        missing: &[BlockId<StarkNetBlockHash>],
    ) -> Result<()> {
        let mut blocks = Vec::with_capacity(missing.len());
        for block_id in missing {
            info!(block_id = ?block_id, "fetch missing block");
            let block = match block_id {
                BlockId::Number(number) => {
                    self.provider
                        .get_block_by_number(&BlockNumOrTag::Number(*number))
                        .await?
                }
                BlockId::HashAndNumber(hash, number) => {
                    let hash_fe = hash.try_into()?;
                    let block = self
                        .provider
                        .get_block_by_hash(&BlockHashOrTag::Hash(hash_fe))
                        .await?;

                    // check the block has the number the tracker expects
                    if block.metadata.block_number != *number {
                        return Err(StarkNetChainTrackerError::BlockNumberMismatch {
                            expected: *number,
                            actual: block.metadata.block_number,
                        });
                    }

                    block
                }
            };

            let block_header = to_block_header(&block)?;
            blocks.push(block_header);
        }
        Ok(self.tracker.supply_missing_blocks(blocks)?)
    }

    async fn handle_block_gap(&self, missing_block: &BlockHeader<StarkNetBlockHash>) -> Result<()> {
        todo!()
    }
}

fn to_block_header(block: &Block) -> Result<BlockHeader<StarkNetBlockHash>> {
    let w: BlockHeaderWrapper = block.try_into()?;
    Ok(w.0)
}

struct BlockHeaderWrapper(BlockHeader<StarkNetBlockHash>);

impl TryFrom<&Block> for BlockHeaderWrapper {
    type Error = StarkNetChainTrackerError;

    fn try_from(b: &Block) -> Result<Self> {
        let hash =
            <StarkNetBlockHash as BlockHash>::from_slice(&b.metadata.block_hash.to_bytes_be())?;
        let parent_hash =
            <StarkNetBlockHash as BlockHash>::from_slice(&b.metadata.parent_hash.to_bytes_be())?;
        let res = BlockHeader {
            hash,
            parent_hash,
            number: b.metadata.block_number,
        };
        Ok(BlockHeaderWrapper(res))
    }
}
