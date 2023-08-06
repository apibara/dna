use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{LoadScriptError, Sink};
use async_trait::async_trait;
use http::{
    header::{InvalidHeaderName, InvalidHeaderValue},
    uri::InvalidUri,
    HeaderMap,
};
use reqwest::Client;
use serde::ser::Serialize;
use serde_json::{json, Value};
use tracing::{debug, info, instrument, warn};

use crate::{configuration::SinkWebhookOptions, SinkWebhookConfiguration};

#[derive(Debug, thiserror::Error)]
pub enum InvalidHeader {
    #[error("Invalid header format")]
    Format,
    #[error("Invalid header name: {0}")]
    Name(#[from] InvalidHeaderName),
    #[error("Invalid header value: {0}")]
    Value(#[from] InvalidHeaderValue),
}

#[derive(Debug, thiserror::Error)]
pub enum SinkWebhookError {
    #[error("Failed to load script: {0}")]
    ScriptLoading(#[from] LoadScriptError),
    #[error("Http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Missing target url")]
    MissingTargetUrl,
    #[error("Invalid header option: {0}")]
    InvalidHeader(#[from] InvalidHeader),
    #[error("Invalid target url: {0}")]
    InvalidUri(#[from] InvalidUri),
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
            .await?;

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
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error> {
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(
            cursor = %cursor_str,
            end_block = %end_cursor,
            finality = ?finality,
            "webhook: calling with data"
        );

        if self.raw {
            self.send(&batch).await
        } else {
            let body = &json!({
                "data": {
                    "cursor": cursor,
                    "end_cursor": end_cursor,
                    "finality": finality,
                    "batch": batch,
                },
            });
            self.send(&body).await
        }
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

        info!(cursor = %cursor_str, "webhook: calling with invalidate");
        let body = json!({
            "invalidate": {
                "cursor": cursor,
            },
        });

        self.send(&body).await
    }
}
