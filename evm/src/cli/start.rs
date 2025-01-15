use apibara_dna_common::{run_server, StartArgs};
use clap::Args;
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{error::EvmError, EvmBlockIngestionOptions, EvmChainSupport};

use super::rpc::RpcArgs;

#[derive(Args, Debug)]
pub struct StartCommand {
    #[clap(flatten)]
    rpc: RpcArgs,
    #[clap(flatten)]
    start: StartArgs,

    /// Do NOT ingest and serve pending blocks.
    #[arg(
        long = "evm.no-ingest-pending",
        env = "EVM_NO_INGEST_PENDING",
        default_value = "false"
    )]
    no_ingest_pending: bool,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        info!("Starting EVM DNA server");
        let provider = self.rpc.to_json_rpc_provider()?;
        let evm_ingestion_options = EvmBlockIngestionOptions {
            ingest_pending: !self.no_ingest_pending,
        };
        let evm_chain = EvmChainSupport::new(provider, evm_ingestion_options);

        run_server(evm_chain, self.start, ct)
            .await
            .change_context(EvmError)
    }
}
