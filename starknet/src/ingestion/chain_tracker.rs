use std::{pin::Pin, sync::Arc, time::Duration};

use apibara_dna_common::{core::Cursor, ingestion::CursorProvider};
use error_stack::{Result, ResultExt};
use futures_util::{Stream, StreamExt};
use tracing::{trace, warn};

use crate::ingestion::provider::models::{BlockExt, BlockIdExt};

use super::{provider::models, JsonRpcProvider, JsonRpcProviderError};

pub struct StarknetCursorProvider {
    provider: Arc<JsonRpcProvider>,
    options: StarknetCursorProviderOptions,
}

#[derive(Debug, Clone, Copy)]
pub struct StarknetCursorProviderOptions {
    pub poll_interval: Duration,
    pub finalized_poll_interval: Duration,
    pub request_timeout: Duration,
}

impl StarknetCursorProvider {
    pub fn new(provider: JsonRpcProvider, options: StarknetCursorProviderOptions) -> Self {
        Self {
            provider: provider.into(),
            options,
        }
    }
}

impl Default for StarknetCursorProviderOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(3),
            finalized_poll_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[async_trait::async_trait]
impl CursorProvider for StarknetCursorProvider {
    type Error = JsonRpcProviderError;
    type CursorStream = Pin<Box<dyn Stream<Item = Cursor> + Send>>;

    async fn subscribe_head(&self) -> Result<Self::CursorStream, Self::Error> {
        let stream = poll_block_id_stream(
            self.provider.clone(),
            models::BlockId::Tag(models::BlockTag::Latest),
            self.options,
        )
        .await?;
        Ok(stream.boxed())
    }

    async fn subscribe_finalized(&self) -> Result<Self::CursorStream, Self::Error> {
        let stream = poll_finalized_block_stream(self.provider.clone(), self.options).await?;
        Ok(stream.boxed())
    }

    async fn get_parent_cursor(&self, cursor: &Cursor) -> Result<Cursor, Self::Error> {
        let block_id = cursor.to_block_id();
        let parent_hash = self
            .provider
            .get_block(block_id)
            .await?
            .parent_hash()
            .to_bytes_be()
            .to_vec();
        Ok(Cursor::new(cursor.number - 1, parent_hash))
    }
}

async fn poll_block_id_stream(
    provider: Arc<JsonRpcProvider>,
    id: models::BlockId,
    options: StarknetCursorProviderOptions,
) -> Result<impl Stream<Item = Cursor>, JsonRpcProviderError> {
    let mut previous = get_block_id(&provider, id, options.request_timeout)
        .await
        .attach_printable("failed to get starting block id")?;

    let stream = async_stream::stream! {
        yield previous.clone();

        loop {
            trace!(?id, "polling block id");
            let cursor = match get_block_id(&provider, id, options.request_timeout).await {
                Ok(cursor) => cursor,
                Err(err) => {
                    warn!(error = ?err, "failed to poll block id");
                    tokio::time::sleep(10 * options.poll_interval).await;
                    continue;
                }
            };

            if cursor != previous {
                yield cursor.clone();
                previous = cursor;
            }

            tokio::time::sleep(options.poll_interval).await;
        }
    };

    Ok(stream)
}

async fn get_block_id(
    provider: &JsonRpcProvider,
    id: models::BlockId,
    timeout: Duration,
) -> Result<Cursor, JsonRpcProviderError> {
    let cursor = tokio::time::timeout(timeout, provider.get_block(id))
        .await
        .change_context(JsonRpcProviderError)
        .attach_printable("timeout sending RPC request")??
        .cursor()
        .ok_or(JsonRpcProviderError)
        .attach_printable("missing block cursor")?;
    Ok(cursor)
}

async fn poll_finalized_block_stream(
    provider: Arc<JsonRpcProvider>,
    options: StarknetCursorProviderOptions,
) -> Result<impl Stream<Item = Cursor>, JsonRpcProviderError> {
    let latest_block_id = models::BlockId::Tag(models::BlockTag::Latest);

    let head = get_block_id(&provider, latest_block_id, options.request_timeout)
        .await
        .attach_printable("failed to get head block")?;

    let finalized = {
        let mut number = head.number - 50;
        loop {
            let id = models::BlockId::Number(number);
            let block = provider.get_block(id).await?;
            if block.is_finalized() {
                let cursor = block.cursor().ok_or(JsonRpcProviderError)?;
                break cursor;
            }

            if number == 0 {
                return Err(JsonRpcProviderError)
                    .attach_printable("failed to find finalized block");
            } else if number < 100 {
                number = 0;
            } else {
                number -= 100;
            }
        }
    };

    let mut previous =
        binary_search_finalized_block(&provider, &finalized, head.number, options.request_timeout)
            .await?;

    trace!(?finalized, ?previous, "starting poll finalized stream");

    let stream = async_stream::stream! {
        yield previous.clone();

        loop {
            trace!("polling finalized block");
            let head = match get_block_id(&provider, latest_block_id, options.request_timeout).await {
                Ok(head) => head,
                Err(err) => {
                    warn!(error = ?err, "failed to poll head for finalized");
                    tokio::time::sleep(2 * options.finalized_poll_interval).await;
                    continue;
                }
            };
            let cursor = match binary_search_finalized_block(&provider, &previous, head.number, options.request_timeout).await {
                Ok(cursor) => cursor,
                Err(err) => {
                    warn!(error = ?err, "failed to poll finalized");
                    tokio::time::sleep(2 * options.finalized_poll_interval).await;
                    continue;
                }
            };

            if cursor != previous {
                yield cursor.clone();
                previous = cursor;
            }

            tokio::time::sleep(options.finalized_poll_interval).await;
        }
    };

    Ok(stream)
}

async fn binary_search_finalized_block(
    provider: &JsonRpcProvider,
    existing_finalized: &Cursor,
    head: u64,
    request_timeout: Duration,
) -> Result<Cursor, JsonRpcProviderError> {
    trace!(
        ?existing_finalized,
        head,
        "binary search for finalized block"
    );
    let mut finalized = existing_finalized.clone();
    let mut head = head;
    let mut step_count = 0;

    loop {
        step_count += 1;

        if step_count > 100 {
            return Err(JsonRpcProviderError)
                .attach_printable("maximum number of iterations reached");
        }

        let mid_block_number = finalized.number + (head - finalized.number) / 2;
        trace!(mid_block_number, "binary search iteration");

        if mid_block_number <= finalized.number {
            trace!(?finalized, "finalized block found");
            break;
        }

        let mid_block = tokio::time::timeout(
            request_timeout,
            provider.get_block(models::BlockId::Number(mid_block_number)),
        )
        .await
        .change_context(JsonRpcProviderError)
        .attach_printable("request timeout")?
        .attach_printable("failed to get block by number")?;

        let mid_cursor = mid_block
            .cursor()
            .ok_or(JsonRpcProviderError)
            .attach_printable("block is missing cursor")?;

        if mid_block.is_finalized() {
            finalized = mid_cursor;
        } else {
            head = mid_cursor.number;
        }
    }

    Ok(finalized)
}
