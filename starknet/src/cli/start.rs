use apibara_dna_common::{run_server, StartArgs};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{error::StarknetError, StarknetBlockIngestionOptions, StarknetChainSupport};

use super::rpc::RpcArgs;

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,

    /// Do NOT ingest and serve pending blocks.
    #[arg(
        long = "starknet.no-ingest-pending",
        env = "STARKNET_NO_INGEST_PENDING",
        default_value = "false"
    )]
    no_ingest_pending: bool,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), StarknetError> {
        info!("Starting Starknet DNA server");
        let provider = self.rpc.to_starknet_provider()?;
        let starknet_ingestion_options = StarknetBlockIngestionOptions {
            ingest_pending: !self.no_ingest_pending,
        };
        let starknet_chain = StarknetChainSupport::new(provider, starknet_ingestion_options);

        run_server(starknet_chain, self.start, ct)
            .await
            .change_context(StarknetError)
    }
}
