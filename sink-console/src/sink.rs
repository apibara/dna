use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{CursorAction, DisplayCursor, Sink};
use async_trait::async_trait;
use serde_json::Value;
use tracing::{info, instrument};

use crate::configuration::SinkConsoleOptions;

#[derive(Debug, thiserror::Error)]
pub enum SinkConsoleError {
    #[error("error serializing batch: {0}")]
    Serialize(#[from] serde_json::Error),
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
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        info!(
            cursor = %DisplayCursor(cursor),
            end_block = %end_cursor,
            finality = ?finality,
            "handle data"
        );

        let pretty = serde_json::to_string_pretty(batch)?;
        info!("{}", pretty);

        Ok(CursorAction::Persist)
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "invalidating cursor");
        Ok(())
    }
}
