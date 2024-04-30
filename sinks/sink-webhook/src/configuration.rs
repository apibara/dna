use apibara_sink_common::SinkOptions;
use apibara_sink_common::{SinkError, SinkErrorResultExt};
use clap::{Args, ValueEnum};
use error_stack::Result;
use http::{HeaderMap, HeaderName, HeaderValue, Uri};
use serde::Deserialize;

#[derive(Debug)]
pub struct SinkWebhookConfiguration {
    pub target_url: Uri,
    pub headers: HeaderMap,
    pub mode: BodyMode,
}

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "webhook")]
pub struct SinkWebhookOptions {
    /// The target url to send the request to.
    #[arg(long, env = "WEBHOOK_TARGET_URL")]
    target_url: Option<String>,

    /// Additional headers to send with the request.
    #[arg(long, short = 'H', value_delimiter = ',', env = "WEBHOOK_HEADERS")]
    header: Option<Vec<String>>,

    /// How to send the body of the request.
    #[arg(long, env = "WEBHOOK_MODE")]
    mode: Option<BodyMode>,
}

#[derive(Debug, Clone, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum BodyMode {
    /// Json, one request per item returned.
    Json,
    /// Newline-delimited json, one request with all items returned.
    Ndjson,
    /// Send the body as plain text.
    Text,
}

impl SinkOptions for SinkWebhookOptions {
    fn merge(self, other: SinkWebhookOptions) -> Self {
        Self {
            target_url: self.target_url.or(other.target_url),
            header: self.header.or(other.header),
            mode: self.mode.or(other.mode),
        }
    }
}

impl SinkWebhookOptions {
    pub fn to_webhook_configuration(self) -> Result<SinkWebhookConfiguration, SinkError> {
        let target_url = self
            .target_url
            .runtime_error("missing target url")?
            .parse::<Uri>()
            .runtime_error("malformed target url")?;

        let headers = match self.header {
            None => HeaderMap::new(),
            Some(headers) => parse_headers(&headers)?,
        };

        Ok(SinkWebhookConfiguration {
            target_url,
            headers,
            mode: self.mode.unwrap_or(BodyMode::Json),
        })
    }
}

fn parse_headers(headers: &[String]) -> Result<HeaderMap, SinkError> {
    let mut new_headers = HeaderMap::new();
    for header in headers {
        match header.split_once(':') {
            None => {
                return Err(SinkError::runtime_error(
                    "header not in the `key: value` format",
                ))
            }
            Some((name, value)) => {
                let name = name
                    .parse::<HeaderName>()
                    .runtime_error("failed to parse header name")?;
                let value = value
                    .parse::<HeaderValue>()
                    .runtime_error("failed to parse header value")?;
                new_headers.append(name, value);
            }
        }
    }

    Ok(new_headers)
}
