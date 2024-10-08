use std::time::Duration;

use clap::Args;
use error_stack::{Result, ResultExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::{
    error::StarknetError,
    provider::{StarknetProvider, StarknetProviderOptions},
};

#[derive(Args, Debug)]
pub struct RpcArgs {
    /// Starknet RPC URL.
    #[arg(
        long = "rpc.url",
        env = "STARKNET_RPC_URL",
        default_value = "http://localhost:8545"
    )]
    pub rpc_url: String,

    /// Request timeout.
    #[arg(
        long = "rpc.timeout-sec",
        env = "STARKNET_RPC_TIMEOUT_SEC",
        default_value = "20"
    )]
    pub rpc_timeout_sec: u64,

    /// Headers to send with the requests.
    #[arg(long = "rpc.headers", env = "STARKNET_RPC_HEADERS")]
    pub rpc_headers: Vec<String>,
}

impl RpcArgs {
    pub fn to_starknet_provider(&self) -> Result<StarknetProvider, StarknetError> {
        let url = self
            .rpc_url
            .parse::<Url>()
            .change_context(StarknetError)
            .attach_printable("failed to parse RPC URL")
            .attach_printable_lazy(|| format!("url: {}", self.rpc_url))?;

        let headers = {
            let mut headers = HeaderMap::default();

            for kv in self.rpc_headers.iter() {
                let (key, value) = kv
                    .split_once(':')
                    .ok_or(StarknetError)
                    .attach_printable("invalid header")
                    .attach_printable_lazy(|| format!("header: {}", kv))?;

                headers.insert(
                    key.parse::<HeaderName>()
                        .change_context(StarknetError)
                        .attach_printable("invalid header name")
                        .attach_printable_lazy(|| format!("header name: {}", key))?,
                    value
                        .parse::<HeaderValue>()
                        .change_context(StarknetError)
                        .attach_printable("invalid header value")
                        .attach_printable_lazy(|| format!("header value: {}", value))?,
                );
            }

            headers
        };

        let options = StarknetProviderOptions {
            timeout: Duration::from_secs(self.rpc_timeout_sec),
            headers,
        };

        StarknetProvider::new(url, options).change_context(StarknetError)
    }
}
