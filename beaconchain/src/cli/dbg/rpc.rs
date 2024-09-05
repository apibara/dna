use std::{
    io::BufWriter,
    path::{Path, PathBuf},
    time::Instant,
};

use clap::Subcommand;
use error_stack::{Result, ResultExt};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    cli::rpc::RpcArgs,
    error::BeaconChainError,
    provider::{http::BlockId, models},
};

#[derive(Subcommand, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum DebugRpcCommand {
    /// Get the header of a block.
    GetHeader {
        #[clap(flatten)]
        rpc: RpcArgs,
        /// The block ID.
        #[arg(long, env, default_value = "head")]
        block_id: String,
        /// Write the response to a JSON file.
        #[arg(long)]
        json: Option<PathBuf>,
    },
    /// Get the block.
    GetBlock {
        #[clap(flatten)]
        rpc: RpcArgs,
        /// The block ID.
        #[arg(long, env, default_value = "head")]
        block_id: String,
        /// Write the response to a JSON file.
        #[arg(long)]
        json: Option<PathBuf>,
    },
    /// Get the blob sidecar.
    GetBlobSidecar {
        #[clap(flatten)]
        rpc: RpcArgs,
        /// The block ID.
        #[arg(long, env, default_value = "head")]
        block_id: String,
        /// Write the response to a JSON file.
        #[arg(long)]
        json: Option<PathBuf>,
    },
    /// Get the validators.
    GetValidators {
        #[clap(flatten)]
        rpc: RpcArgs,
        /// The block ID.
        #[arg(long, env, default_value = "head")]
        block_id: String,
        /// Write the response to a JSON file.
        #[arg(long)]
        json: Option<PathBuf>,
    },
}

/// Data needed to assemble a block.
#[derive(Serialize, Deserialize)]
pub(crate) enum JsonBlock {
    Missed { slot: u64 },
    Proposed(ProposedBlock),
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ProposedBlock {
    pub block: models::BeaconBlock,
    pub blob_sidecars: Vec<models::BlobSidecar>,
    pub validators: Vec<models::Validator>,
}

impl DebugRpcCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        let rpc_provider = self
            .rpc_provider()
            .change_context(BeaconChainError)
            .attach_printable("failed to create RPC provider")?;

        let block_id = self.block_id()?;

        let start = Instant::now();
        let elapsed = match self {
            DebugRpcCommand::GetHeader { json, .. } => {
                info!(block_id = ?block_id, "getting header");
                let header = rpc_provider
                    .get_header(block_id)
                    .await
                    .change_context(BeaconChainError)?;
                info!("received {:#?}", header);

                let elapsed = start.elapsed();
                if let Some(json_path) = json {
                    write_json(json_path, &header)?;
                }
                elapsed
            }
            DebugRpcCommand::GetBlock { json, .. } => {
                info!(block_id = ?block_id, "getting block");
                let block = rpc_provider
                    .get_block(block_id)
                    .await
                    .change_context(BeaconChainError)?;
                info!("received {:#?}", block);

                let elapsed = start.elapsed();
                if let Some(json_path) = json {
                    write_json(json_path, &block)?;
                }
                elapsed
            }
            DebugRpcCommand::GetBlobSidecar { json, .. } => {
                info!(block_id = ?block_id, "getting blob sidecar");
                let sidecar = rpc_provider
                    .get_blob_sidecar(block_id)
                    .await
                    .change_context(BeaconChainError)?;
                info!("received {:#?}", sidecar);

                let elapsed = start.elapsed();
                if let Some(json_path) = json {
                    write_json(json_path, &sidecar)?;
                }
                elapsed
            }
            DebugRpcCommand::GetValidators { json, .. } => {
                info!(block_id = ?block_id, "getting validators");
                let validators = rpc_provider
                    .get_validators(block_id)
                    .await
                    .change_context(BeaconChainError)?;
                info!("received {:#?}", validators);

                let elapsed = start.elapsed();
                if let Some(json_path) = json {
                    write_json(json_path, &validators)?;
                }
                elapsed
            }
        };

        info!(elapsed = ?elapsed, "debug rpc command completed");

        Ok(())
    }

    fn rpc_provider(&self) -> Result<crate::provider::http::BeaconApiProvider, BeaconChainError> {
        match self {
            DebugRpcCommand::GetHeader { rpc, .. } => rpc.to_beacon_api_provider(),
            DebugRpcCommand::GetBlock { rpc, .. } => rpc.to_beacon_api_provider(),
            DebugRpcCommand::GetBlobSidecar { rpc, .. } => rpc.to_beacon_api_provider(),
            DebugRpcCommand::GetValidators { rpc, .. } => rpc.to_beacon_api_provider(),
        }
    }

    fn block_id(&self) -> Result<BlockId, BeaconChainError> {
        let block_id = match self {
            DebugRpcCommand::GetHeader { block_id, .. } => block_id,
            DebugRpcCommand::GetBlock { block_id, .. } => block_id,
            DebugRpcCommand::GetBlobSidecar { block_id, .. } => block_id,
            DebugRpcCommand::GetValidators { block_id, .. } => block_id,
        };

        match block_id.as_str() {
            "head" => Ok(BlockId::Head),
            "finalized" => Ok(BlockId::Finalized),
            str_value => {
                if let Ok(slot) = str_value.parse::<u64>() {
                    return Ok(BlockId::Slot(slot));
                }
                if let Ok(block_root) = str_value.parse::<models::B256>() {
                    return Ok(BlockId::BlockRoot(block_root));
                }
                Err(BeaconChainError)
                    .attach_printable_lazy(|| format!("invalid block id: {}", str_value))
            }
        }
    }
}

fn write_json(path: impl AsRef<Path>, data: &impl Serialize) -> Result<(), BeaconChainError> {
    use std::fs::File;
    use std::io::Write;

    let path = path.as_ref();
    let file = File::create(path).change_context(BeaconChainError)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, data)
        .change_context(BeaconChainError)
        .attach_printable("failed to write JSON")
        .attach_printable_lazy(|| format!("path: {}", path.display()))?;
    writer.flush().change_context(BeaconChainError)?;

    Ok(())
}
