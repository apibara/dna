use std::time::Duration;

use clap::Args;
use error_stack::{Result, ResultExt};
use reqwest::Url;
use url::ParseError;

use crate::provider::http::{BeaconApiProvider, BeaconApiProviderOptions};

#[derive(Args, Clone, Debug)]
pub struct RpcArgs {
    /// Beacon RPC URL.
    #[arg(long = "rpc.url", env, default_value = "http://localhost:3500")]
    pub rpc_url: String,
    #[arg(long = "rpc.timeout-sec", env, default_value = "20")]
    pub rpc_timeout_sec: u64,
    #[arg(long = "rpc.validators-timeout-sec", env, default_value = "180")]
    pub rpc_validators_timeout_sec: u64,
}

impl RpcArgs {
    pub fn to_beacon_api_provider(&self) -> Result<BeaconApiProvider, ParseError> {
        let url = self
            .rpc_url
            .parse::<Url>()
            .attach_printable("failed to parse RPC URL")
            .attach_printable_lazy(|| format!("url: {}", self.rpc_url))?;

        let options = BeaconApiProviderOptions {
            timeout: Duration::from_secs(self.rpc_timeout_sec),
            validators_timeout: Duration::from_secs(self.rpc_validators_timeout_sec),
        };

        Ok(BeaconApiProvider::new(url, options))
    }
}
