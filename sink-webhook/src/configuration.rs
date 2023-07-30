use apibara_sink_common::SinkOptions;
use clap::Args;
use http::{HeaderMap, HeaderName, Uri};
use serde::Deserialize;

use crate::sink::{InvalidHeader, SinkWebhookError};

#[derive(Debug)]
pub struct SinkWebhookConfiguration {
    pub target_url: Uri,
    pub headers: HeaderMap,
    pub raw: bool,
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

    /// Send the data received from the transform step as is.
    ///
    /// Use this to interact with any API like Discord or Telegram.
    #[arg(long, action, env = "WEBHOOK_RAW")]
    raw: Option<bool>,
}

impl SinkOptions for SinkWebhookOptions {
    fn merge(self, other: SinkWebhookOptions) -> Self {
        Self {
            target_url: self.target_url.or(other.target_url),
            header: self.header.or(other.header),
            raw: self.raw.or(other.raw),
        }
    }
}

impl SinkWebhookOptions {
    pub fn to_webhook_configuration(self) -> Result<SinkWebhookConfiguration, SinkWebhookError> {
        let target_url: Uri = self
            .target_url
            .ok_or_else(|| SinkWebhookError::MissingTargetUrl)?
            .parse()?;

        let headers = match self.header {
            None => HeaderMap::new(),
            Some(headers) => parse_headers(&headers)?,
        };

        Ok(SinkWebhookConfiguration {
            target_url,
            headers,
            raw: self.raw.unwrap_or(false),
        })
    }
}

fn parse_headers(headers: &[String]) -> Result<HeaderMap, InvalidHeader> {
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
    Ok(new_headers)
}
