use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use apibara_node::{
    db::{
        libmdbx::{Environment, Geometry, NoWriteMap},
        node_data_dir, DatabaseClapCommandExt,
    },
    otel::{init_opentelemetry, OpenTelemetryClapCommandExt},
};
use clap::{command, ArgMatches, Command};

use apibara_starknet::StarkNetSourceNode;

async fn start(start_matches: &ArgMatches) -> Result<()> {
    init_opentelemetry()?;

    // Setup database
    let datadir = start_matches
        .get_one::<PathBuf>("datadir")
        .map(|p| Some(p.clone()))
        .unwrap_or_else(|| node_data_dir("starknet"))
        .ok_or_else(|| anyhow!("could not get datadir"))?;
    fs::create_dir_all(&datadir)?;
    let min_size = byte_unit::n_gib_bytes!(10) as usize;
    let max_size = byte_unit::n_gib_bytes!(100) as usize;
    let growth_step = byte_unit::n_gib_bytes(2) as isize;
    let db = Environment::<NoWriteMap>::new()
        .set_geometry(Geometry {
            size: Some(min_size..max_size),
            growth_step: Some(growth_step),
            shrink_threshold: None,
            page_size: None,
        })
        .set_max_dbs(100)
        .open(&datadir)?;
    let db = Arc::new(db);

    let node = StarkNetSourceNode::new(db);
    node.start().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            Command::new("start")
                .about("Start a StarkNet source node")
                .data_dir_args(),
        )
        .open_telemetry_args()
        .get_matches();

    match matches.subcommand() {
        Some(("start", start_matches)) => start(start_matches).await,
        _ => unreachable!(),
    }
}
