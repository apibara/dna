use anyhow::Result;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use apibara::server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let (server_handle, server_addr) = server::start_server()?;

    info!("gRPC server started: {:?}", server_addr);
    server_handle.await?;
    Ok(())
}
