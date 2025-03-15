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

    /// Ingest transaction traces.
    #[arg(
        long = "evm.ingest-traces",
        env = "EVM_INGEST_TRACES",
        default_value = "false"
    )]
    ingest_traces: bool,
}

impl StartCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        info!("Starting EVM DNA server");
        let provider = self.rpc.to_json_rpc_provider()?;
        let evm_ingestion_options = EvmBlockIngestionOptions {
            ingest_pending: !self.no_ingest_pending,
            ingest_traces: self.ingest_traces,
        };
        let evm_chain = EvmChainSupport::new(provider, evm_ingestion_options);

        run_server(evm_chain, self.start, env!("CARGO_PKG_VERSION"), ct)
            .await
            .change_context(EvmError)
    }
}
