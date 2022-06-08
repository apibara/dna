use anyhow::{Context, Result};
use starknet_rpc::RpcProvider;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let _provider = RpcProvider::new("http://localhost:9545")
        .context("failed to create starknet rpc provider")?;

    Ok(())
}
