use std::fs;

use byte_unit::Byte;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use rkyv::{option::ArchivedOption, Deserialize};
use tracing::info;

use crate::{
    error::BeaconChainError,
    provider::models,
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
    /// Dump the content of the header segment.
    Header {
        #[clap(flatten)]
        segment: SegmentArgs,
        /// Whether to show the body.
        #[clap(long)]
        show_body: bool,
    },
    /// Dump the content of the transaction segment.
    Transaction {
        #[clap(flatten)]
        segment: SegmentArgs,
        /// Whether to show the index.
        #[clap(long)]
        show_index: bool,
        /// Whether to show the body.
        #[clap(long)]
        show_body: bool,
    },
    /// Dump the content of the validator segment.
    Validator {
        #[clap(flatten)]
        segment: SegmentArgs,
        /// Whether to show the index.
        #[clap(long)]
        show_index: bool,
        /// Whether to show the body.
        #[clap(long)]
        show_body: bool,
    },
    /// Dump the content of the blob segment.
    Blob {
        #[clap(flatten)]
        segment: SegmentArgs,
        /// Whether to show the body.
        #[clap(long)]
        show_body: bool,
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
            TextDumpCommand::Header {
                segment: args,
                show_body,
            } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<HeaderSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("==========================================================");
                info!("segment dump of {}", args.file);
                info!("first block number: {}", segment.first_block_number);
                info!("num blocks: {}", segment.body.len());
                info!("==========================================================");
                for (block_index, block) in segment.body.iter().enumerate() {
                    info!("== {:<6}", block_index);

                    let fragment::ArchivedSlot::Proposed(block) = block else {
                        info!("  MISSED");
                        continue;
                    };
                    info!(
                        "  CURSOR: number={} hash=0x{}...",
                        block.cursor.number,
                        &hex::encode(&block.cursor.hash)[0..8]
                    );

                    if show_body {
                        info!("  -- HEADER");
                        info!("    SLOT: {}", block.data.slot);
                        info!(
                            "    PARENT ROOT: 0x{}...",
                            &hex::encode(block.data.parent_root.0)[0..16]
                        );
                        info!(
                            "    STATE ROOT: 0x{}...",
                            &hex::encode(block.data.state_root.0)[0..16]
                        );
                        info!("    BLOB COUNT: {}", block.data.blob_kzg_commitments.len());
                        if let ArchivedOption::Some(execution_payload) =
                            &block.data.execution_payload
                        {
                            info!("    EXECUTION PAYLOAD");
                            info!("      NUMBER {}", execution_payload.block_number);
                            info!("      TIMESTAMP {}", execution_payload.timestamp);
                        } else {
                            info!("    EXECUTION PAYLOAD: NONE");
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Transaction {
                segment: args,
                show_index,
                show_body,
            } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<TransactionSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("==========================================================");
                info!("segment dump of {}", args.file);
                info!("first block number: {}", segment.first_block_number);
                info!("num blocks: {}", segment.body.len());
                info!("==========================================================");
                for (block_index, block) in segment.body.iter().enumerate() {
                    info!("== {:<6}", block_index);

                    let fragment::ArchivedSlot::Proposed(block) = block else {
                        info!("  MISSED");
                        continue;
                    };
                    info!(
                        "  CURSOR: number={} hash=0x{}...",
                        block.cursor.number,
                        &hex::encode(&block.cursor.hash)[0..8]
                    );

                    if show_index {
                        info!("  -- INDEX BY FROM ADDRESS");
                        let index = &block.index.by_from_address;
                        for entry in index.iter() {
                            let bitmap =
                                entry.value.deserialize().change_context(BeaconChainError)?;
                            info!("  0x{}", hex::encode(entry.key.0));
                            info!("    count: {}", bitmap.len());
                            info!(
                                "    size: {:#}",
                                Byte::from_u64(bitmap.serialized_size() as u64)
                            );
                            info!("    bitmap: {:#?}", bitmap);
                        }

                        info!("  -- INDEX BY TO ADDRESS");
                        let index = &block.index.by_to_address;
                        for entry in index.iter() {
                            let bitmap =
                                entry.value.deserialize().change_context(BeaconChainError)?;
                            info!("  0x{}", hex::encode(entry.key.0));
                            info!("    count: {}", bitmap.len());
                            info!(
                                "    size: {:#}",
                                Byte::from_u64(bitmap.serialized_size() as u64)
                            );
                            info!("    bitmap: {:#?}", bitmap);
                        }

                        info!("  -- INDEX BY CREATE");
                        let index = &block.index.by_create;
                        let bitmap = index.deserialize().change_context(BeaconChainError)?;
                        info!("  count: {}", bitmap.len());
                        info!(
                            "  size: {:#}",
                            Byte::from_u64(bitmap.serialized_size() as u64)
                        );
                        info!("  bitmap: {:#?}", bitmap);
                    }

                    if show_body {
                        for (transaction_index, transaction) in block.data.iter().enumerate() {
                            info!("  -- TX {:<4}", transaction_index);
                            info!("    TRANSACTION INDEX: {}", transaction.transaction_index);
                            info!(
                                "    TRANSACTION HASH: 0x{}...",
                                &hex::encode(transaction.transaction_hash.0)[0..20]
                            );
                            info!("    FROM: 0x{}", hex::encode(transaction.from.0));
                            if let ArchivedOption::Some(to) = &transaction.to {
                                info!("    TO: 0x{}", hex::encode(to.0));
                            } else {
                                info!("    TO: CREATE");
                            }
                            info!("    NONCE: {}", transaction.nonce);
                            info!(
                                "    BLOB COUNT: {}",
                                transaction.blob_versioned_hashes.len()
                            );
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Validator {
                segment: args,
                show_index,
                show_body,
            } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<ValidatorSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("==========================================================");
                info!("segment dump of {}", args.file);
                info!("first block number: {}", segment.first_block_number);
                info!("num blocks: {}", segment.body.len());
                info!("==========================================================");
                for (block_index, block) in segment.body.iter().enumerate() {
                    info!("== {:<6}", block_index);

                    let fragment::ArchivedSlot::Proposed(block) = block else {
                        info!("  MISSED");
                        continue;
                    };
                    info!(
                        "  CURSOR: number={} hash=0x{}...",
                        block.cursor.number,
                        &hex::encode(&block.cursor.hash)[0..8]
                    );

                    if show_index {
                        info!("  -- INDEX BY STATUS");
                        let index = &block.index.by_status;
                        for entry in index.iter() {
                            let bitmap =
                                entry.value.deserialize().change_context(BeaconChainError)?;
                            let status: models::ValidatorStatus = entry
                                .key
                                .deserialize(&mut rkyv::Infallible)
                                .change_context(BeaconChainError)?;
                            info!("  {:?}", status);
                            info!("    count: {}", bitmap.len());
                            info!(
                                "    size: {:#}",
                                Byte::from_u64(bitmap.serialized_size() as u64)
                            );
                            info!("    bitmap: {:#?}", bitmap);
                        }
                    }

                    if show_body {
                        for (validator_index, validator) in block.data.iter().enumerate() {
                            let status: models::ValidatorStatus = validator
                                .status
                                .deserialize(&mut rkyv::Infallible)
                                .change_context(BeaconChainError)?;

                            info!("  -- VALIDATOR {:<4}", validator_index);
                            info!("    INDEX: {}", validator.validator_index);
                            info!("    STATUS: {:?}", status);
                            info!("    BALANCE: {}", validator.balance);
                            info!(
                                "    PUBKEY: 0x{}...",
                                &hex::encode(validator.pubkey.0)[0..20]
                            );
                        }
                    }
                }

                Ok(())
            }
            TextDumpCommand::Blob {
                segment: args,
                show_body,
            } => {
                let bytes = fs::read(&args.file).change_context(BeaconChainError)?;
                let segment = rkyv::check_archived_root::<BlobSegment>(&bytes)
                    .map_err(|_| BeaconChainError)
                    .attach_printable("failed to deserialize segment")?;

                info!("==========================================================");
                info!("segment dump of {}", args.file);
                info!("first block number: {}", segment.first_block_number);
                info!("num blocks: {}", segment.body.len());
                info!("==========================================================");
                for (block_index, block) in segment.body.iter().enumerate() {
                    info!("== {:<6}", block_index);

                    let fragment::ArchivedSlot::Proposed(block) = block else {
                        info!("  MISSED");
                        continue;
                    };
                    info!(
                        "  CURSOR: number={} hash=0x{}...",
                        block.cursor.number,
                        &hex::encode(&block.cursor.hash)[0..8]
                    );

                    if show_body {
                        for (blob_index, blob) in block.data.iter().enumerate() {
                            info!("  -- BLOB {:<4}", blob_index);
                            info!("    BLOB INDEX: {}", blob.blob_index);
                            info!(
                                "    BLOB HASH: 0x{}...",
                                &hex::encode(blob.blob_hash.0)[0..20]
                            );
                            info!("    BLOB SIZE: {}", blob.blob.0.len());
                            info!(
                                "    KZG COMMITMENT: 0x{}...",
                                &hex::encode(blob.kzg_commitment.0)[0..20]
                            );
                            info!(
                                "    KZG PROOF: 0x{}...",
                                &hex::encode(blob.kzg_proof.0)[0..20]
                            );
                        }
                    }
                }

                Ok(())
            }
        }
    }
}
