use alloy_primitives::hex::FromHex;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    cli::rpc::RpcArgs,
    error::EvmError,
    provider::{models, BlockId, JsonRpcProvider},
};

#[derive(Subcommand, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum DebugRpcCommand {
    /// Get a block with full transactions.
    GetBlockWithTransactions {
        #[clap(flatten)]
        rpc: RpcArgs,
        #[arg(long, env, default_value = "head")]
        block_id: String,
    },
    /// Get a block with its receipts.
    GetBlockReceipts {
        #[clap(flatten)]
        rpc: RpcArgs,
        #[arg(long, env, default_value = "head")]
        block_id: String,
    },
}

impl DebugRpcCommand {
    pub async fn run(self) -> Result<(), EvmError> {
        let rpc_provider = self.rpc_provider()?;
        let block_id = self.block_id()?;

        match self {
            DebugRpcCommand::GetBlockWithTransactions { .. } => {
                info!(block_id = ?block_id, "getting block with transactions");
                let block_with_transactions = rpc_provider
                    .get_block_with_transactions(block_id)
                    .await
                    .change_context(EvmError)?;

                println!("{:#?}", block_with_transactions);

                Ok(())
            }
            DebugRpcCommand::GetBlockReceipts { .. } => {
                info!(block_id = ?block_id, "getting block receipts");
                let block_receipts = rpc_provider
                    .get_block_receipts(block_id)
                    .await
                    .change_context(EvmError)?;

                println!("{:#?}", block_receipts);

                Ok(())
            }
        }
    }

    fn rpc_provider(&self) -> Result<JsonRpcProvider, EvmError> {
        match self {
            DebugRpcCommand::GetBlockWithTransactions { rpc, .. } => rpc.to_json_rpc_provider(),
            DebugRpcCommand::GetBlockReceipts { rpc, .. } => rpc.to_json_rpc_provider(),
        }
    }

    fn block_id(&self) -> Result<BlockId, EvmError> {
        let block_id = match self {
            DebugRpcCommand::GetBlockWithTransactions { block_id, .. } => block_id,
            DebugRpcCommand::GetBlockReceipts { block_id, .. } => block_id,
        };

        match block_id.as_str() {
            "head" => Ok(BlockId::latest()),
            "finalized" => Ok(BlockId::finalized()),
            str_value => {
                if let Ok(number) = str_value.parse::<u64>() {
                    return Ok(BlockId::Number(number.into()));
                }
                if let Ok(hash) = models::B256::from_hex(str_value) {
                    return Ok(BlockId::Hash(hash.into()));
                }
                Err(EvmError)
                    .attach_printable("invalid block id")
                    .attach_printable_lazy(|| format!("block id: {}", block_id))
            }
        }
    }
}
