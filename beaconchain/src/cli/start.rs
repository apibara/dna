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
    /// Whether to ingest validators or not.
    #[clap(
        long = "beaconchain.ingest-validators",
        env = "BEACONCHAIN_INGEST_VALIDATORS"
    )]
    pub ingest_validators: bool,
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
            ingest_validators: self.ingest_validators,
        }
    }
}
