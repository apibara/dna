use std::time::Duration;

use backon::ExponentialBuilder;
use clap::Args;
use error_stack::{Result, ResultExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::{
    error::EvmError,
    provider::{JsonRpcProvider, JsonRpcProviderOptions},
};

#[derive(Args, Debug)]
pub struct RpcArgs {
    /// Evm RPC URL.
    #[arg(
        long = "rpc.url",
        env = "EVM_RPC_URL",
        default_value = "http://localhost:9545"
    )]
    pub rpc_url: String,

    /// Request timeout.
    #[arg(
        long = "rpc.timeout-sec",
        env = "EVM_RPC_TIMEOUT_SEC",
        default_value = "20"
    )]
    pub rpc_timeout_sec: u64,

    /// Headers to send with the requests.
    #[arg(long = "rpc.headers", env = "EVM_RPC_HEADERS")]
    pub rpc_headers: Vec<String>,
}

impl RpcArgs {
    pub fn to_json_rpc_provider(&self) -> Result<JsonRpcProvider, EvmError> {
        let url = self
            .rpc_url
            .parse::<Url>()
            .change_context(EvmError)
            .attach_printable("failed to parse RPC URL")
            .attach_printable_lazy(|| format!("url: {}", self.rpc_url))?;

        let headers = {
            let mut headers = HeaderMap::default();

            for kv in self.rpc_headers.iter() {
                let (key, value) = kv
                    .split_once(':')
                    .ok_or(EvmError)
                    .attach_printable("invalid header")
                    .attach_printable_lazy(|| format!("header: {}", kv))?;

                headers.insert(
                    key.parse::<HeaderName>()
                        .change_context(EvmError)
                        .attach_printable("invalid header name")
                        .attach_printable_lazy(|| format!("header name: {}", key))?,
                    value
                        .parse::<HeaderValue>()
                        .change_context(EvmError)
                        .attach_printable("invalid header value")
                        .attach_printable_lazy(|| format!("header value: {}", value))?,
                );
            }

            headers
        };

        let timeout = Duration::from_secs(self.rpc_timeout_sec);
        let max_delay = Duration::from_secs(self.rpc_timeout_sec / 2);
        let options = JsonRpcProviderOptions {
            timeout,
            headers,
            exponential_backoff: ExponentialBuilder::default().with_max_delay(max_delay),
        };

        JsonRpcProvider::new(url, options).change_context(EvmError)
    }
}
