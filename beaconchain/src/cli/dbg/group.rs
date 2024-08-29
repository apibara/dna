use std::fs;

use apibara_dna_common::store::group::SegmentGroup;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    cli::dbg::helpers::print_index,
    error::BeaconChainError,
    store::block::{
        IndexTransactionByCreate, IndexTransactionByFromAddress, IndexTransactionByToAddress,
        IndexValidatorByStatus,
    },
};

#[derive(Subcommand, Debug)]
pub enum DebugGroupCommand {
    /// Dump the content of a group.
    TextDump {
        /// Path to the group file.
        #[clap(long)]
        file: String,
    },
}

impl DebugGroupCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            DebugGroupCommand::TextDump { file } => {
                let bytes = fs::read(&file).change_context(BeaconChainError)?;
                let group = rkyv::check_archived_root::<SegmentGroup>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment group")?;

                info!("segment group dump of {}", file);
                info!("first block number {}", group.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&group.first_block.hash)
                );

                print_index::<IndexTransactionByFromAddress>(&group.index)?;
                print_index::<IndexTransactionByToAddress>(&group.index)?;
                print_index::<IndexTransactionByCreate>(&group.index)?;
                print_index::<IndexValidatorByStatus>(&group.index)?;

                Ok(())
            }
        }
    }
}
