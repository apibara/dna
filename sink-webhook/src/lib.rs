use std::str::FromStr;

use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::Sink;
use async_trait::async_trait;
use http::{
    header::{InvalidHeaderName, InvalidHeaderValue},
    uri::InvalidUri,
    HeaderMap, HeaderName, Uri,
};
use reqwest::Client;
use serde::ser::Serialize;
use serde_json::{json, Value};
use tracing::{debug, info, instrument, warn};

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("Http error: {0}")]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidHeader {
    #[error("Invalid header format")]
    Format,
    #[error("Invalid header name: {0}")]
    Name(#[from] InvalidHeaderName),
    #[error("Invalid header value: {0}")]
    Value(#[from] InvalidHeaderValue),
}

pub struct WebhookSink {
    client: Client,
    target_url: String,
    headers: HeaderMap,
    raw: bool,
}

impl WebhookSink {
    pub fn new(target_url: String, raw: bool) -> Result<Self, InvalidUri> {
        let _ = Uri::from_str(&target_url)?;
        Ok(Self {
            client: Client::new(),
            target_url,
            headers: HeaderMap::new(),
            raw: raw,
        })
    }

    pub fn with_headers(mut self, headers: &[String]) -> Result<Self, InvalidHeader> {
        let mut new_headers = HeaderMap::new();
        for header in headers {
            match header.split_once(':') {
                None => return Err(InvalidHeader::Format),
                Some((name, value)) => {
                    let name: HeaderName = name.parse()?;
                    let value = value.parse()?;
                    new_headers.append(name, value);
                }
            }
        }

        self.headers = new_headers;
        Ok(self)
    }

    #[instrument(skip(self, body), err(Debug))]
    async fn send<B: Serialize + ?Sized>(&self, body: &B) -> Result<(), WebhookError> {
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
    type Error = WebhookError;

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
