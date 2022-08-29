use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use apibara_node::{
    db::{
        libmdbx::{Environment, NoWriteMap},
        node_data_dir, DatabaseClapCommandExt, MdbxEnvironmentExt,
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
    let db = Environment::<NoWriteMap>::open(&datadir)?;
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

    /*
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let path = Path::new("/tmp/foo");
    let db = Environment::<NoWriteMap>::open(path)?;
    let db = Arc::new(db);
    let node = StarkNetSourceNode::new(db)?;
    node.start().await?;
    Ok(())
    */
}
