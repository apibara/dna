use apibara_dna_common::{run_server, StartArgs};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    cli::rpc::RpcArgs, error::BeaconChainError, BeaconChainChainSupport, BeaconChainOptions,
};

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    beaconchain: BeaconChainArgs,
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,
}

#[derive(Args, Clone, Debug)]
pub struct BeaconChainArgs {
    /// Collect validators at this block interval.
    ///
    /// Defaults to sampling at the beginning of each epoch (every 32 blocks).
    /// Set to `1` to disable sampling, and set to `0` to not ingest validators.
    #[clap(
        long = "beaconchain.sample-validators",
        env = "BEACON_SAMPLE_VALIDATORS",
        default_value = "32"
    )]
    pub sample_validators: u64,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BeaconChainError> {
        info!("Starting Beaconchain DNA server");
        let provider = self.rpc.to_beacon_api_provider()?;
        let options = self.beaconchain.to_beacon_chain_options();
        let beaconchain_chain = BeaconChainChainSupport::new(provider, options);

        run_server(beaconchain_chain, self.start, env!("CARGO_PKG_VERSION"), ct)
            .await
            .change_context(BeaconChainError)
    }
}

impl BeaconChainArgs {
    pub fn to_beacon_chain_options(&self) -> BeaconChainOptions {
        BeaconChainOptions {
            sample_validators: self.sample_validators,
        }
    }
}
