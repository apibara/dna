use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct RpcArgs {
    /// Starknet RPC URL.
    #[arg(long, env)]
    pub rpc_url: String,
    /// RPC rate limit, in requests per second.
    #[arg(long, env, default_value = "100")]
    pub rpc_rate_limit: usize,
    /// How many concurrent requests to send.
    #[arg(long, env, default_value = "10")]
    pub rpc_concurrency: usize,
}
