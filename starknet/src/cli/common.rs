use apibara_dna_common::error::Result;
use clap::Args;

use crate::ingestion::RpcProviderService;

#[derive(Args, Debug, Clone)]
pub struct RpcArgs {
    /// Starknet RPC URL.
    #[arg(long, env)]
    pub rpc_url: String,
    /// RPC rate limit, in requests per second.
    #[arg(long, env, default_value = "1000")]
    pub rpc_rate_limit: usize,
    /// How many concurrent requests to send.
    #[arg(long, env, default_value = "100")]
    pub rpc_concurrency: usize,
}

impl RpcArgs {
    pub fn to_provider_service(&self) -> Result<RpcProviderService> {
        let provider = RpcProviderService::new(&self.rpc_url)?
            .with_rate_limit(self.rpc_rate_limit as u32)
            .with_concurrency(self.rpc_concurrency);
        Ok(provider)
    }
}
