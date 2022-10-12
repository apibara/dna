mod app;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};

use apibara_node::{
    db::{
        libmdbx::{Environment, NoWriteMap},
        node_data_dir, MdbxEnvironmentExt,
    },
    node::Node,
};

use crate::app::SimpleApplication;

#[tokio::main]
async fn main() -> Result<()> {
    let app = SimpleApplication {};
    let datadir = node_data_dir("starknet-token-transfer-example")
        .ok_or_else(|| anyhow!("could not get datadir"))?;
    let db = Environment::<NoWriteMap>::builder()
        .with_size_gib(2, 20)
        .open(&datadir)?;
    let db = Arc::new(db);
    let address: SocketAddr = "0.0.0.0:7172".parse()?;
    Node::new(db, app)?.with_address(address).start().await?;
    Ok(())
}
