use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, Result};
use apibara_node::{
    db::{node_data_dir, DatabaseClapCommandExt},
    o11y::{init_opentelemetry, OpenTelemetryClapCommandExt},
};
use apibara_starknet::{start_starknet_source_node, SequencerGateway};
use clap::{arg, command, value_parser, ArgGroup, ArgMatches, Command};
use tokio_util::sync::CancellationToken;

async fn start(start_matches: &ArgMatches) -> Result<()> {
    init_opentelemetry()?;

    // Setup database
    let datadir = start_matches
        .get_one::<String>("datadir")
        .map(|p| Some(PathBuf::from(p)))
        .unwrap_or_else(|| node_data_dir("starknet"))
        .ok_or_else(|| anyhow!("could not get datadir"))?;

    let gateway = {
        if let Some(custom) = start_matches.get_one::<String>("custom-network") {
            SequencerGateway::from_custom_str(custom)?
        } else if start_matches.is_present("testnet") {
            SequencerGateway::GoerliTestnet
        } else {
            SequencerGateway::Mainnet
        }
    };

    let poll_interval = start_matches.get_one::<u64>("poll-interval").unwrap(); // safe to unwrap as format is checked by clap
    let poll_interval = Duration::from_millis(*poll_interval);

    // Setup cancellation for graceful shutdown
    let cts = CancellationToken::new();
    ctrlc::set_handler({
        let cts = cts.clone();
        move || {
            cts.cancel();
        }
    })?;

    Ok(start_starknet_source_node(datadir, gateway, poll_interval, cts.clone()).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = command!()
        .subcommand_required(true)
        .subcommand(
            Command::new("start")
                .about("Start a StarkNet source node")
                .data_dir_args()
                .arg(arg!(--testnet "StarkNet Goerli test network").required(false))
                .arg(arg!(--mainnet "StarkNet mainnet network").required(false))
                .arg(arg!(--"custom-network" <URL> "Custom StarkNet network").required(false))
                .arg(
                    arg!(--"poll-interval" "Custom poll interval in ms")
                        .required(false)
                        .value_parser(value_parser!(u64))
                        .default_value("5000"),
                )
                .group(
                    ArgGroup::new("sequencer_gateway")
                        .args(&["testnet", "mainnet", "custom-network"])
                        .multiple(false),
                ),
        )
        .open_telemetry_args()
        .get_matches();

    match matches.subcommand() {
        Some(("start", start_matches)) => start(start_matches).await,
        _ => unreachable!(),
    }
}
