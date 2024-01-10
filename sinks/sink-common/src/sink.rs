use std::fmt::Display;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use async_trait::async_trait;
use error_stack::Result;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::cursor::DisplayCursor;

pub trait SinkOptions: DeserializeOwned {
    fn merge(self, other: Self) -> Self;
}

#[derive(Debug, PartialEq)]
pub enum CursorAction {
    Persist,
    Skip,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub cursor: Option<Cursor>,
    pub end_cursor: Cursor,
    pub finality: DataFinality,
}

#[async_trait]
pub trait Sink {
    type Options: SinkOptions;
    type Error: error_stack::Context + Send + Sync + 'static;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error>
    where
        Self: Sized;

    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error>;

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error>;

    async fn cleanup(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn handle_heartbeat(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = DisplayCursor(&self.cursor);
        write!(
            f,
            "Context(start={}, end={}, finality={:?})",
            start, self.end_cursor, self.finality
        )
    }
}
