use apibara_dna_protocol::dna::common::Cursor;
use apibara_sink_common::{Context, CursorAction, DisplayCursor, Sink};
use apibara_sink_common::{SinkError, SinkErrorResultExt};
use async_trait::async_trait;
use error_stack::Result;
use serde_json::Value;
use tracing::{debug, info, instrument};

use crate::configuration::SinkConsoleOptions;

#[derive(Default)]
pub struct ConsoleSink {}

impl ConsoleSink {}

#[async_trait]
impl Sink for ConsoleSink {
    type Options = SinkConsoleOptions;
    type Error = SinkError;

    async fn from_options(_options: Self::Options) -> Result<Self, Self::Error> {
        Ok(ConsoleSink::default())
    }

    #[instrument(skip_all, err(Debug), level = "DEBUG")]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        debug!(ctx = %ctx, "handle data");

        let pretty =
            serde_json::to_string_pretty(batch).runtime_error("failed to serialize batch data")?;

        info!("{}", pretty);

        Ok(CursorAction::Persist)
    }

    #[instrument(skip_all, err(Debug), level = "DEBUG")]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "invalidating cursor");
        Ok(())
    }
}
