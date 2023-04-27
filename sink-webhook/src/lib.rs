use std::str::FromStr;

use apibara_core::{
    node::v1alpha2::{Cursor, DataFinality},
    starknet::v1alpha2::Block,
};
use apibara_sink_common::Sink;
use async_trait::async_trait;
use http::{uri::InvalidUri, Uri};
use reqwest::Client;
use serde::ser::Serialize;
use serde_json::json;
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("Http error: {0}")]
    Http(#[from] reqwest::Error),
}

pub struct WebhookSink {
    client: Client,
    target_url: String,
}

impl WebhookSink {
    pub fn new(target_url: String) -> Result<Self, InvalidUri> {
        let _ = Uri::from_str(&target_url)?;
        Ok(Self {
            client: Client::new(),
            target_url,
        })
    }

    async fn send<B: Serialize + ?Sized>(&self, body: &B) -> Result<(), WebhookError> {
        let response = self.client.post(&self.target_url).json(body).send().await?;

        match response.text().await {
            Ok(text) => {
                debug!(response = ?text, "call success");
            }
            Err(err) => {
                warn!(err = ?err, "error reading response");
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Sink<Block> for WebhookSink {
    type Error = WebhookError;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &[Block],
    ) -> Result<(), Self::Error> {
        info!(cursor = ?cursor, end_cursor = ?end_cursor, "calling web hook with data");
        let body = json!({
            "data": {
                "cursor": cursor,
                "end_cursor": end_cursor,
                "finality": finality,
                "batch": batch,
            },
        });

        self.send(&body).await
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = ?cursor, "calling web hook with invalidate");
        let body = json!({
            "invalidate": {
                "cursor": cursor,
            },
        });

        self.send(&body).await
    }
}
