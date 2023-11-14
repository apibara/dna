use std::fmt;

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{Context, CursorAction, Sink};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use http::HeaderMap;
use reqwest::Client;
use serde::ser::Serialize;
use serde_json::{json, Value};
use tracing::{debug, info, instrument, warn};

use crate::{configuration::SinkWebhookOptions, SinkWebhookConfiguration};

#[derive(Debug)]
pub struct SinkWebhookError;
impl error_stack::Context for SinkWebhookError {}

impl fmt::Display for SinkWebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("webhook sink operation failed")
    }
}

pub struct WebhookSink {
    client: Client,
    target_url: String,
    headers: HeaderMap,
    raw: bool,
}

impl WebhookSink {
    pub fn new(config: SinkWebhookConfiguration) -> Self {
        Self {
            client: Client::new(),
            target_url: config.target_url.to_string(),
            headers: config.headers,
            raw: config.raw,
        }
    }

    #[instrument(skip(self, body), err(Debug))]
    async fn send<B: Serialize + ?Sized>(&self, body: &B) -> Result<(), SinkWebhookError> {
        let response = self
            .client
            .post(&self.target_url)
            .headers(self.headers.clone())
            .json(body)
            .send()
            .await
            .change_context(SinkWebhookError)
            .attach_printable("failed to POST json data")?;

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
impl Sink for WebhookSink {
    type Options = SinkWebhookOptions;
    type Error = SinkWebhookError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        let config = options.to_webhook_configuration()?;
        Ok(WebhookSink::new(config))
    }

    #[instrument(skip(self, batch), err(Debug))]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        info!(ctx = %ctx, "calling with data");

        if self.raw {
            // Send each item returned by the transform script as a separate request
            let Some(batch) = batch.as_array() else {
                warn!("raw mode: batch is not an array");
                return Ok(CursorAction::Persist);
            };

            for item in batch {
                self.send(&item).await?;
            }
        } else {
            let body = &json!({
                "data": {
                    "cursor": ctx.cursor,
                    "end_cursor": ctx.end_cursor,
                    "finality": ctx.finality,
                    "batch": batch,
                },
            });
            self.send(&body).await?;
        }

        Ok(CursorAction::Persist)
    }

    #[instrument(skip(self), err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        if self.raw {
            return Ok(());
        }

        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(cursor = %cursor_str, "calling with invalidate");
        let body = json!({
            "invalidate": {
                "cursor": cursor,
            },
        });

        self.send(&body).await
    }
}
