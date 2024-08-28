use std::fs;

use apibara_dna_common::store::segment::IndexSegment;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    error::BeaconChainError,
    segment::{BlobSegment, HeaderSegment, TransactionSegment, ValidatorSegment},
    store::fragment,
};

#[derive(Subcommand, Debug)]
pub enum DebugSegmentCommand {
    /// Dump the content of a segment.
    TextDump {
        #[clap(subcommand)]
        command: TextDumpCommand,
    },
}

#[derive(Subcommand, Debug)]
pub enum TextDumpCommand {
    /// Dump the content of the index segment.
    Index {
        #[clap(flatten)]
        segment: SegmentArgs,
    },
    /// Dump the content of the header segment.
    Header {
        #[clap(flatten)]
        segment: SegmentArgs,
    },
    /// Dump the content of the transaction segment.
    Transaction {
        #[clap(flatten)]
        segment: SegmentArgs,
    },
    /// Dump the content of the validator segment.
    Validator {
        #[clap(flatten)]
        segment: SegmentArgs,
    },
    /// Dump the content of the blob segment.
    Blob {
        #[clap(flatten)]
        segment: SegmentArgs,
    },
}

#[derive(clap::Args, Debug)]
pub struct SegmentArgs {
    /// Path to the segment file.
    #[clap(long)]
    pub file: String,
}

impl DebugSegmentCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            DebugSegmentCommand::TextDump { command } => command.run().await,
        }
    }
}

impl TextDumpCommand {
    async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            TextDumpCommand::Index { segment: args } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<IndexSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("segment dump of {}", args.file);
                info!("first block number {}", segment.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&segment.first_block.hash)
                );
                info!("block count: {}", segment.blocks.len());

                for (block_index, block) in segment.blocks.iter().enumerate() {
                    info!("* offset: {}", block_index);
                    info!("  index count: {}", block.tags.len());
                }

                Ok(())
            }
            TextDumpCommand::Header { segment: args } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<HeaderSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("segment dump of {}", args.file);
                info!("first block number {}", segment.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&segment.first_block.hash)
                );
                info!("block count: {}", segment.blocks.len());

                for (block_index, block) in segment.blocks.iter().enumerate() {
                    info!("* offset: {}", block_index);
                    match block {
                        fragment::ArchivedSlot::Missed { slot } => {
                            info!("  missed slot {}", slot);
                        }
                        fragment::ArchivedSlot::Proposed(block) => {
                            info!("  proposed slot {}", block.slot);
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Transaction { segment: args } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<TransactionSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("segment dump of {}", args.file);
                info!("first block number {}", segment.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&segment.first_block.hash)
                );
                info!("block count: {}", segment.blocks.len());

                for (block_index, block) in segment.blocks.iter().enumerate() {
                    info!("* offset: {}", block_index);
                    match block {
                        fragment::ArchivedSlot::Missed { slot } => {
                            info!("  missed slot {}", slot);
                        }
                        fragment::ArchivedSlot::Proposed(_block) => {
                            info!("  proposed slot");
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Validator { segment: args } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<ValidatorSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("segment dump of {}", args.file);
                info!("first block number {}", segment.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&segment.first_block.hash)
                );
                info!("block count: {}", segment.blocks.len());

                for (block_index, block) in segment.blocks.iter().enumerate() {
                    info!("* offset: {}", block_index);
                    match block {
                        fragment::ArchivedSlot::Missed { slot } => {
                            info!("  missed slot {}", slot);
                        }
                        fragment::ArchivedSlot::Proposed(_block) => {
                            info!("  proposed slot");
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Blob { segment: args } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<BlobSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("segment dump of {}", args.file);
                info!("first block number {}", segment.first_block.number);
                info!(
                    "first block hash 0x{}",
                    hex::encode(&segment.first_block.hash)
                );
                info!("block count: {}", segment.blocks.len());

                for (block_index, block) in segment.blocks.iter().enumerate() {
                    info!("* offset: {}", block_index);
                    match block {
                        fragment::ArchivedSlot::Missed { slot } => {
                            info!("  missed slot {}", slot);
                        }
                        fragment::ArchivedSlot::Proposed(_block) => {
                            info!("  proposed slot");
                        }
                    }
                }

                Ok(())
            }
        }
    }
}
