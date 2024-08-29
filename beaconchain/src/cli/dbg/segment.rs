use std::{fs, path::PathBuf};

use apibara_dna_common::store::{group::SegmentGroupBuilder, segment::IndexSegment};
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    error::BeaconChainError,
    segment::{BlobSegment, HeaderSegment, TransactionSegment, ValidatorSegment},
    store::{
        block::{
            IndexTransactionByCreate, IndexTransactionByFromAddress, IndexTransactionByToAddress,
            IndexValidatorByStatus,
        },
        fragment,
    },
};

use super::helpers::print_index;

#[derive(Subcommand, Debug)]
pub enum DebugSegmentCommand {
    CreateGroup {
        /// Segments to create a group from.
        segments: Vec<PathBuf>,
        /// Path to the output file.
        #[arg(long)]
        out: PathBuf,
    },
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
        /// Print the index in a human-readable format.
        #[arg(long, default_value = "false")]
        display: bool,
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
            DebugSegmentCommand::CreateGroup { segments, out } => {
                let mut builder = SegmentGroupBuilder::default();

                for segment in segments {
                    let bytes = fs::read(&segment).change_context(BeaconChainError)?;
                    let segment = rkyv::check_archived_root::<IndexSegment>(&bytes)
                        .map_err(|_| BeaconChainError)
                        .attach_printable("failed to deserialize segment")?;

                    builder
                        .add_archived_segment(segment)
                        .change_context(BeaconChainError)
                        .attach_printable("failed to add segment to group")?;
                }

                let segment_group = builder.build().change_context(BeaconChainError)?;
                let data =
                    rkyv::to_bytes::<_, 0>(&segment_group).change_context(BeaconChainError)?;

                fs::write(out, &data)
                    .change_context(BeaconChainError)
                    .attach_printable("failed to write segment group")?;

                Ok(())
            }
        }
    }
}

impl TextDumpCommand {
    async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            TextDumpCommand::Index {
                segment: args,
                display,
            } => {
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
                    info!(
                        "  cursor: number={} hash=0x{}",
                        block.cursor.number,
                        hex::encode(&block.cursor.hash)
                    );
                    info!("  index count: {}", block.data.tags.len());

                    if !display {
                        continue;
                    }

                    print_index::<IndexTransactionByFromAddress>(&block.data)?;
                    print_index::<IndexTransactionByToAddress>(&block.data)?;
                    print_index::<IndexTransactionByCreate>(&block.data)?;
                    print_index::<IndexValidatorByStatus>(&block.data)?;
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
                    info!(
                        "  cursor: number={} hash=0x{}",
                        block.cursor.number,
                        hex::encode(&block.cursor.hash)
                    );
                    match &block.data {
                        fragment::ArchivedSlot::Missed { .. } => {
                            info!("  missed slot");
                        }
                        fragment::ArchivedSlot::Proposed(block) => {
                            info!(
                                "  proposed slot: hash=0x{}",
                                hex::encode(block.state_root.0)
                            );
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
                    info!(
                        "  cursor: number={} hash=0x{}",
                        block.cursor.number,
                        hex::encode(&block.cursor.hash)
                    );
                    match &block.data {
                        fragment::ArchivedSlot::Missed { .. } => {
                            info!("  missed slot");
                        }
                        fragment::ArchivedSlot::Proposed(block) => {
                            info!("  proposed slot: transactions={}", block.len());
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
                    info!(
                        "  cursor: number={} hash=0x{}",
                        block.cursor.number,
                        hex::encode(&block.cursor.hash)
                    );
                    match &block.data {
                        fragment::ArchivedSlot::Missed { .. } => {
                            info!("  missed slot");
                        }
                        fragment::ArchivedSlot::Proposed(block) => {
                            info!("  proposed slot: validators={}", block.len());
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
                    info!(
                        "  cursor: number={} hash=0x{}",
                        block.cursor.number,
                        hex::encode(&block.cursor.hash)
                    );
                    match &block.data {
                        fragment::ArchivedSlot::Missed { .. } => {
                            info!("  missed slot");
                        }
                        fragment::ArchivedSlot::Proposed(block) => {
                            info!("  proposed slot: blobs={}", block.len());
                        }
                    }
                }

                Ok(())
            }
        }
    }
}
