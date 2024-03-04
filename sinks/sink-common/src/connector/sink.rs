use apibara_dna_protocol::dna::Cursor;
use error_stack::{Result, ResultExt};
use exponential_backoff::Backoff;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    error::SinkError,
    sink::{Context, Sink},
    CursorAction, SinkErrorReportExt,
};

pub struct SinkWithBackoff<S: Sink + Send + Sync> {
    inner: S,
    backoff: Backoff,
}

impl<S: Sink + Send + Sync> SinkWithBackoff<S> {
    pub fn new(inner: S, backoff: Backoff) -> Self {
        Self { inner, backoff }
    }

    pub async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
        ct: CancellationToken,
    ) -> Result<CursorAction, SinkError> {
        for duration in &self.backoff {
            match self.inner.handle_data(ctx, batch).await {
                Ok(action) => return Ok(action),
                Err(err) => {
                    warn!(err = ?err, "failed to handle data");
                    if ct.is_cancelled() {
                        return Err(err)
                            .change_context(SinkError::Fatal)
                            .attach_printable("failed to handle data (cancelled)");
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                        },
                        _ = ct.cancelled() => {
                            return Ok(CursorAction::Skip);
                        }
                    };
                }
            }
        }

        Err(SinkError::Fatal).attach_printable("handle data failed after retry")
    }

    pub async fn handle_replace(
        &mut self,
        ctx: &Context,
        batch: &Value,
        ct: CancellationToken,
    ) -> Result<CursorAction, SinkError> {
        for duration in &self.backoff {
            match self.inner.handle_replace(ctx, batch).await {
                Ok(action) => return Ok(action),
                Err(err) => {
                    warn!(err = ?err, "failed to handle data");
                    if ct.is_cancelled() {
                        return Err(err)
                            .change_context(SinkError::Fatal)
                            .attach_printable("failed to handle replace data (cancelled)");
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {
                        },
                        _ = ct.cancelled() => {
                            return Ok(CursorAction::Skip);
                        }
                    };
                }
            }
        }

        Err(SinkError::Fatal).attach_printable("handle replace data failed after retry")
    }

    pub async fn handle_invalidate(
        &mut self,
        cursor: &Option<Cursor>,
        ct: CancellationToken,
    ) -> Result<(), SinkError> {
        for duration in &self.backoff {
            match self.inner.handle_invalidate(cursor).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    warn!(err = ?err, "failed to handle invalidate");
                    if ct.is_cancelled() {
                        return Err(err)
                            .change_context(SinkError::Fatal)
                            .attach_printable("failed to handle invalidate (cancelled)");
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {},
                        _ = ct.cancelled() => {
                            return Ok(());
                        }
                    };
                }
            }
        }

        Err(SinkError::Fatal).attach_printable("handle invalidate failed after retry")
    }

    pub async fn cleanup(&mut self) -> Result<(), SinkError> {
        self.inner
            .cleanup()
            .await
            .map_err(|err| err.temporary("failed to cleanup sink"))?;
        Ok(())
    }

    pub async fn handle_heartbeat(&mut self) -> Result<(), SinkError> {
        self.inner
            .handle_heartbeat()
            .await
            .map_err(|err| err.temporary("failed to handle heartbeat"))?;
        Ok(())
    }
}
