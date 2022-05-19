use std::sync::Arc;

use anyhow::{Context, Result};
use ethers::prelude::{Http, Provider};

use apibara::ethereum::EthereumBlockHeaderProvider;
use apibara::BlockHeaderProvider;

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Provider::<Http>::try_from("http://localhost:8545")
        .context("failed to create ethereum provider")?;
    let provider = Arc::new(provider);
    let block_header_provider = EthereumBlockHeaderProvider::new(provider);

    let genesis = block_header_provider
        .get_block_by_number(0)
        .await
        .context("failed to fetch genesis block")?;

    println!("genesis = {:?}", genesis);

    let head = block_header_provider
        .get_head_block()
        .await
        .context("failed to fetch head block")?;

    println!("head = {:?}", head);

    let by_hash = block_header_provider
        .get_block_by_hash(&head.hash)
        .await
        .context("failed to fetch block by hash")?;

    println!("by_hash = {:?}", by_hash);

    Ok(())
}
