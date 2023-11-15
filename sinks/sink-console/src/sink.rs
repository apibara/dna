use std::fmt;

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{Context, CursorAction, DisplayCursor, Sink};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use serde_json::Value;
use tracing::{debug, info, instrument};

use crate::configuration::SinkConsoleOptions;

#[derive(Debug)]
pub struct SinkConsoleError;
impl error_stack::Context for SinkConsoleError {}

impl fmt::Display for SinkConsoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("console sink operation failed")
    }
}

#[derive(Default)]
pub struct ConsoleSink {}

impl ConsoleSink {}

#[async_trait]
impl Sink for ConsoleSink {
    type Options = SinkConsoleOptions;
    type Error = SinkConsoleError;

    async fn from_options(_options: Self::Options) -> Result<Self, Self::Error> {
        Ok(ConsoleSink::default())
    }

    #[instrument(skip(self, batch), err(Debug), level = "DEBUG")]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        debug!(ctx = %ctx, "handle data");

        let pretty = serde_json::to_string_pretty(batch)
            .change_context(SinkConsoleError)
            .attach_printable("failed to serialize batch data")?;

        info!("{}", pretty);

        Ok(CursorAction::Persist)
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "invalidating cursor");
        Ok(())
    }
}
