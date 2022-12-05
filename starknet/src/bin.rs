use std::time::Duration;

use anyhow::{anyhow, Result};
use apibara_node::{
    db::{node_data_dir, DatabaseClapCommandExt},
    o11y::{init_opentelemetry, OpenTelemetryClapCommandExt},
};
use apibara_starknet::{HttpProvider, NoWriteMap, StarkNetNode};
use clap::{arg, command, value_parser, ArgMatches, Command};
use tokio_util::sync::CancellationToken;

async fn start(start_matches: &ArgMatches) -> Result<()> {
    init_opentelemetry()?;

    let url = start_matches
        .get_one::<String>("rpc")
        .ok_or_else(|| anyhow!("expected rpc argument"))?;

    let mut node = StarkNetNode::<HttpProvider, NoWriteMap>::builder(url)?;

    // use specified datadir
    if let Some(name) = start_matches.get_one::<String>("name") {
        let datadir =
            node_data_dir(name).ok_or_else(|| anyhow!("failed to create node data dir"))?;
        node.with_datadir(datadir);
    }

    let poll_interval = start_matches.get_one::<u64>("poll-interval").unwrap(); // safe to unwrap as format is checked by clap
    let poll_interval = Duration::from_millis(*poll_interval);
    node.with_poll_interval(poll_interval);

    // Setup cancellation for graceful shutdown
    let cts = CancellationToken::new();
    ctrlc::set_handler({
        let cts = cts.clone();
        move || {
            cts.cancel();
        }
    })?;

    node.build()?.start(cts.clone()).await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            Command::new("start")
                .about("Start a StarkNet source node")
                .data_dir_args()
                .arg(arg!(--rpc <URL> "StarkNet RPC url").required(true))
                .arg(arg!(--name <NAME> "Indexer name").required(false))
                .arg(
                    arg!(--"poll-interval" "Custom poll interval in ms")
                        .required(false)
                        .value_parser(value_parser!(u64))
                        .default_value("5000"),
                ),
        )
        .open_telemetry_args()
        .get_matches();

    match matches.subcommand() {
        Some(("start", start_matches)) => start(start_matches).await,
        _ => unreachable!(),
    }
}
