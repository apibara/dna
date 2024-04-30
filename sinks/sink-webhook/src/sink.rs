use apibara_dna_protocol::dna::common::Cursor;
use apibara_sink_common::{Context, CursorAction, Sink};
use apibara_sink_common::{SinkError, SinkErrorResultExt};
use async_trait::async_trait;
use error_stack::Result;
use http::HeaderMap;
use reqwest::{Body, Client, Response};
use serde::ser::Serialize;
use serde_json::Value;
use tracing::{debug, instrument, warn};

use crate::{configuration::SinkWebhookOptions, BodyMode, SinkWebhookConfiguration};

pub struct WebhookSink {
    client: Client,
    target_url: String,
    headers: HeaderMap,
    mode: BodyMode,
}

impl WebhookSink {
    pub fn new(config: SinkWebhookConfiguration) -> Self {
        Self {
            client: Client::new(),
            target_url: config.target_url.to_string(),
            headers: config.headers,
            mode: config.mode,
        }
    }

    #[instrument(skip(self, body), err(Debug))]
    async fn send_with_json<B: Serialize + ?Sized>(&self, body: &B) -> Result<(), SinkError> {
        let response = self
            .client
            .post(&self.target_url)
            .headers(self.headers.clone())
            .json(body)
            .send()
            .await
            .runtime_error("failed to POST json data")?;

        self.handle_response(response).await
    }

    #[instrument(skip(self, body), err(Debug))]
    async fn send_with_body(&self, body: impl Into<Body>) -> Result<(), SinkError> {
        let response = self
            .client
            .post(&self.target_url)
            .headers(self.headers.clone())
            .body(body)
            .send()
            .await
            .runtime_error("failed to POST data")?;

        self.handle_response(response).await
    }

    async fn handle_response(&self, response: Response) -> Result<(), SinkError> {
        let status = response.status();
        let text = response
            .text()
            .await
            .runtime_error("failed to read response text")?;

        if status.is_success() {
            debug!(status = ?status, response = ?text, "success");
        } else {
            warn!(status = ?status, response = ?text, "error");
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for WebhookSink {
    type Options = SinkWebhookOptions;
    type Error = SinkError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        let config = options.to_webhook_configuration()?;
        Ok(WebhookSink::new(config))
    }

    #[instrument(skip_all, err(Debug))]
    async fn handle_data(
        &mut self,
        ctx: &Context,
        data: &Value,
    ) -> Result<CursorAction, Self::Error> {
        debug!(ctx = %ctx, "calling with data");

        match self.mode {
            BodyMode::Json => match data {
                Value::Array(batch) => {
                    for item in batch {
                        self.send_with_json(&item).await?;
                    }
                }
                Value::Object(_) => {
                    self.send_with_json(data).await?;
                }
                _ => {
                    warn!("json mode: data is not an object or array");
                    return Ok(CursorAction::Persist);
                }
            },
            BodyMode::Ndjson => {
                let Some(batch) = data.as_array() else {
                    warn!("ndjson mode: data is not an array");
                    return Ok(CursorAction::Persist);
                };

                let data = batch
                    .iter()
                    .map(|item| serde_json::to_string(item).fatal("failed to serialize item"))
                    .collect::<Result<Vec<String>, _>>()?
                    .join("\n");
                self.send_with_body(data).await?;
            }
            BodyMode::Text => {
                let Some(body) = data.as_str() else {
                    warn!("text mode: data is not a string");
                    return Ok(CursorAction::Persist);
                };
                self.send_with_body(body.to_string()).await?;
            }
        }

        Ok(CursorAction::Persist)
    }

    #[instrument(skip_all, err(Debug))]
    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        warn!(cursor = ?cursor, "chain reorganization detected");
        Ok(())
    }
}
