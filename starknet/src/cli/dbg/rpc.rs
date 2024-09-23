use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    cli::rpc::RpcArgs,
    error::StarknetError,
    provider::{models, BlockId, StarknetProvider},
};

#[derive(Subcommand, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum DebugRpcCommand {
    /// Get a block with transactions and receipts.
    GetBlockWithReceipts {
        #[clap(flatten)]
        rpc: RpcArgs,
        #[arg(long, env, default_value = "head")]
        block_id: String,
    },
}

impl DebugRpcCommand {
    pub async fn run(self) -> Result<(), StarknetError> {
        let rpc_provider = self.rpc_provider()?;
        let block_id = self.block_id()?;

        match self {
            DebugRpcCommand::GetBlockWithReceipts { .. } => {
                info!(block_id = ?block_id, "getting block with receipts");
                let block_with_receipts = rpc_provider
                    .get_block_with_receipts(&block_id)
                    .await
                    .change_context(StarknetError)?;

                println!("{:#?}", block_with_receipts);

                Ok(())
            }
        }
    }

    fn rpc_provider(&self) -> Result<StarknetProvider, StarknetError> {
        match self {
            DebugRpcCommand::GetBlockWithReceipts { rpc, .. } => rpc.to_starknet_provider(),
        }
    }

    fn block_id(&self) -> Result<BlockId, StarknetError> {
        let block_id = match self {
            DebugRpcCommand::GetBlockWithReceipts { block_id, .. } => block_id,
        };

        match block_id.as_str() {
            "head" => Ok(BlockId::Head),
            str_value => {
                if let Ok(number) = str_value.parse::<u64>() {
                    return Ok(BlockId::Number(number));
                }
                if let Ok(hash) = models::FieldElement::from_hex(str_value) {
                    return Ok(BlockId::Hash(hash));
                }
                Err(StarknetError)
                    .attach_printable("invalid block id")
                    .attach_printable_lazy(|| format!("block id: {}", block_id))
            }
        }
    }
}
