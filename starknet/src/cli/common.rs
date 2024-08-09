use std::sync::Arc;

use clap::Args;
use error_stack::{Result, ResultExt};
use starknet::providers::Url;
use tokio::sync::Semaphore;

use crate::{
    error::DnaStarknetError,
    ingestion::{JsonRpcProviderFactory, JsonRpcProviderOptions},
};

#[derive(Args, Debug, Clone)]
pub struct RpcArgs {
    /// Starknet RPC URL.
    #[arg(long, env)]
    pub rpc_url: String,
    /// RPC rate limit, in requests per second.
    #[arg(long, env, default_value = "1000")]
    pub rpc_rate_limit: u32,
    /// How many concurrent requests to send.
    #[arg(long, env, default_value = "100")]
    pub rpc_concurrency: usize,
}

impl RpcArgs {
    pub fn to_json_rpc_provider_factory(&self) -> Result<JsonRpcProviderFactory, DnaStarknetError> {
        let url = self
            .rpc_url
            .parse::<Url>()
            .change_context(DnaStarknetError::Configuration)
            .attach_printable("failed to parse RPC URL")
            .attach_printable_lazy(|| format!("url: {}", self.rpc_url))?;
        let semaphore = Arc::new(Semaphore::new(self.rpc_concurrency));
        let options = JsonRpcProviderOptions {
            rate_limit: self.rpc_rate_limit,
        };

        Ok(JsonRpcProviderFactory::new(url, options, semaphore))
    }
}
