use std::{pin::Pin, sync::Arc, time::Duration};

use apibara_dna_common::{core::Cursor, ingestion::CursorProvider};
use error_stack::{Result, ResultExt};
use futures_util::{Stream, StreamExt};
use tracing::{trace, warn};

use crate::ingestion::models::BlockIdExt;

use super::{
    models::{self, BlockExt},
    provider::JsonRpcProviderError,
    JsonRpcProvider,
};

pub struct EvmCursorProvider {
    provider: Arc<JsonRpcProvider>,
    options: EvmCursorProviderOptions,
}

#[derive(Debug, Clone, Copy)]
pub struct EvmCursorProviderOptions {
    pub poll_interval: Duration,
    pub request_timeout: Duration,
}

impl EvmCursorProvider {
    pub fn new(provider: JsonRpcProvider, options: EvmCursorProviderOptions) -> Self {
        Self {
            provider: provider.into(),
            options,
        }
    }
}

impl Default for EvmCursorProviderOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
        }
    }
}

#[async_trait::async_trait]
impl CursorProvider for EvmCursorProvider {
    type Error = JsonRpcProviderError;
    type CursorStream = Pin<Box<dyn Stream<Item = Cursor> + Send>>;

    async fn subscribe_head(&self) -> Result<Self::CursorStream, Self::Error> {
        let stream = poll_block_id_stream(
            self.provider.clone(),
            models::BlockId::Number(models::BlockNumberOrTag::Latest),
            self.options,
        )
        .await?;
        Ok(stream.boxed())
    }

    /// Subscribe to changes to the current finalized block.
    async fn subscribe_finalized(&self) -> Result<Self::CursorStream, Self::Error> {
        let stream = poll_block_id_stream(
            self.provider.clone(),
            models::BlockId::Number(models::BlockNumberOrTag::Finalized),
            self.options,
        )
        .await?;
        Ok(stream.boxed())
    }

    /// Returns the cursor of the parent of the provided cursor.
    async fn get_parent_cursor(&self, cursor: &Cursor) -> Result<Cursor, Self::Error> {
        let block_id = cursor.to_block_id();
        let block = self
            .provider
            .get_block(block_id)
            .await?
            .ok_or(JsonRpcProviderError)
            .attach_printable("missing block")?;
        let hash = block.header.parent_hash.to_vec();
        Ok(Cursor::new(cursor.number - 1, hash))
    }
}

async fn poll_block_id_stream(
    provider: Arc<JsonRpcProvider>,
    id: models::BlockId,
    options: EvmCursorProviderOptions,
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
        .ok_or(JsonRpcProviderError)
        .attach_printable("missing block")?
        .cursor()
        .ok_or(JsonRpcProviderError)
        .attach_printable("missing block cursor")?;
    Ok(cursor)
}
