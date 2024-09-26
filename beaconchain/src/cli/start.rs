use apibara_dna_common::{run_server, StartArgs};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{cli::rpc::RpcArgs, error::BeaconChainError, BeaconChainChainSupport};

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BeaconChainError> {
        info!("Starting Beaconchain DNA server");
        let provider = self.rpc.to_beacon_api_provider()?;
        let beaconchain_chain = BeaconChainChainSupport::new(provider);

        run_server(beaconchain_chain, self.start, ct)
            .await
            .change_context(BeaconChainError)
    }
}
