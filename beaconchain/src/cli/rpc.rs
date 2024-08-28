use std::time::Duration;

use clap::Args;
use error_stack::{Result, ResultExt};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Url,
};

use crate::{
    error::BeaconChainError,
    provider::http::{BeaconApiProvider, BeaconApiProviderOptions},
};

#[derive(Args, Clone, Debug)]
pub struct RpcArgs {
    /// Beacon RPC URL.
    #[arg(
        long = "rpc.url",
        env = "BEACON_RPC_URL",
        default_value = "http://localhost:3500"
    )]
    pub rpc_url: String,

    /// Timeout for normal requests.
    #[arg(
        long = "rpc.timeout-sec",
        env = "BEACON_RPC_TIMEOUT_SEC",
        default_value = "20"
    )]
    pub rpc_timeout_sec: u64,

    /// Timeout for validators requests.
    #[arg(
        long = "rpc.validators-timeout-sec",
        env = "BEACON_RPC_VALIDATORS_TIMEOUT_SEC",
        default_value = "180"
    )]
    pub rpc_validators_timeout_sec: u64,

    /// Headers to send with the requests.
    #[arg(long = "rpc.headers", env = "BEACON_RPC_HEADERS")]
    pub rpc_headers: Vec<String>,
}

impl RpcArgs {
    pub fn to_beacon_api_provider(&self) -> Result<BeaconApiProvider, BeaconChainError> {
        let url = self
            .rpc_url
            .parse::<Url>()
            .change_context(BeaconChainError)
            .attach_printable("failed to parse RPC URL")
            .attach_printable_lazy(|| format!("url: {}", self.rpc_url))?;

        let headers = {
            let mut headers = HeaderMap::default();

            for kv in self.rpc_headers.iter() {
                let (key, value) = kv
                    .split_once(':')
                    .ok_or(BeaconChainError)
                    .attach_printable("invalid header")
                    .attach_printable_lazy(|| format!("header: {}", kv))?;

                headers.insert(
                    key.parse::<HeaderName>()
                        .change_context(BeaconChainError)
                        .attach_printable("invalid header name")
                        .attach_printable_lazy(|| format!("header name: {}", key))?,
                    value
                        .parse::<HeaderValue>()
                        .change_context(BeaconChainError)
                        .attach_printable("invalid header value")
                        .attach_printable_lazy(|| format!("header value: {}", value))?,
                );
            }

            headers
        };

        let options = BeaconApiProviderOptions {
            timeout: Duration::from_secs(self.rpc_timeout_sec),
            validators_timeout: Duration::from_secs(self.rpc_validators_timeout_sec),
            headers,
        };

        Ok(BeaconApiProvider::new(url, options))
    }
}
