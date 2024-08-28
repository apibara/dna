use std::{
    fs,
    io::BufReader,
    path::{Path, PathBuf},
};

use byte_unit::Byte;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    error::BeaconChainError,
    ingestion::{add_transaction_to_blobs, decode_transaction},
    segment::SegmentBuilder,
    store::{self, fragment},
};

use super::rpc::JsonBlock;

#[derive(Subcommand, Debug)]
pub enum DebugStoreCommand {
    /// Assemble a single block fragment from the RPC types.
    Assemble {
        /// Path to the JSON file containing the block.
        #[arg(long)]
        json: PathBuf,
        /// Path to the output file.
        #[arg(long)]
        out: PathBuf,
        /// Print file stats.
        #[arg(long, default_value = "false")]
        stats: bool,
    },
    /// Create a segment from indexed blocks.
    CreateSegment {
        /// Files with the indexed blocks.
        files: Vec<PathBuf>,
        #[arg(long)]
        /// Path to the output directory.
        #[arg(long)]
        out_dir: PathBuf,
    },
}

impl DebugStoreCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self {
            DebugStoreCommand::Assemble { json, out, stats } => {
                let json_block = read_json(json).change_context(BeaconChainError)?;

                let block = match json_block {
                    JsonBlock::Missed { slot } => store::fragment::Slot::Missed { slot },
                    JsonBlock::Proposed(mut json_block) => {
                        let transactions = if let Some(ref mut execution_payload) =
                            json_block.block.body.execution_payload
                        {
                            std::mem::take(&mut execution_payload.transactions)
                        } else {
                            Vec::new()
                        };

                        let header = fragment::BlockHeader::from(json_block.block);

                        let transactions = transactions
                            .into_iter()
                            .enumerate()
                            .map(|(tx_index, bytes)| decode_transaction(tx_index, &bytes))
                            .collect::<Result<Vec<_>, _>>()
                            .change_context(BeaconChainError)
                            .attach_printable("failed to decode transactions")?;

                        let mut blobs = json_block
                            .blob_sidecars
                            .into_iter()
                            .map(fragment::Blob::from)
                            .collect::<Vec<_>>();

                        add_transaction_to_blobs(&mut blobs, &transactions)
                            .change_context(BeaconChainError)
                            .attach_printable("failed to add transactions to blobs")?;

                        let validators = json_block
                            .validators
                            .into_iter()
                            .map(fragment::Validator::from)
                            .collect::<Vec<_>>();

                        let mut block_builder = store::block::BlockBuilder::new(header);
                        block_builder.add_transactions(transactions);
                        block_builder.add_validators(validators);
                        block_builder.add_blobs(blobs);
                        let block = block_builder.build().change_context(BeaconChainError)?;

                        store::fragment::Slot::Proposed(block)
                    }
                };

                let bytes = rkyv::to_bytes::<_, 0>(&block)
                    .change_context(BeaconChainError)
                    .attach_printable("failed to serialize block")?;

                if stats {
                    match block {
                        store::fragment::Slot::Missed { slot } => {
                            info!("block {} missed", slot);
                        }
                        store::fragment::Slot::Proposed(block) => {
                            info!("block {} stats", block.header.slot);
                            info!("state root: 0x{}", hex::encode(block.header.state_root.0));
                            info!("parent root: 0x{}", hex::encode(block.header.parent_root.0));
                            info!("transactions: {}", block.transactions.len());
                            info!("validators: {}", block.validators.len());
                            info!("blobs: {}", block.blobs.len());
                        }
                    }
                }

                fs::write(&out, &bytes)
                    .change_context(BeaconChainError)
                    .attach_printable("failed to write block")?;

                info!(
                    "wrote {:#} to {}",
                    Byte::from_u64(bytes.len() as u64),
                    out.display()
                );

                Ok(())
            }
            DebugStoreCommand::CreateSegment { files, out_dir } => {
                let mut files = files.into_iter();

                let Some(file) = files.next() else {
                    info!("no files specified");
                    return Ok(());
                };

                info!("reading first block from {}", file.display());
                let bytes = fs::read(&file).change_context(BeaconChainError)?;
                let block =
                    rkyv::from_bytes::<'_, store::fragment::Slot<store::block::Block>>(&bytes)
                        .map_err(|_| BeaconChainError)
                        .attach_printable("failed to deserialize block")?;

                let first_block = block.cursor();
                let mut segment_builder = SegmentBuilder::new(&first_block);
                segment_builder.add_block(block);

                for file in files {
                    info!("reading block from {}", file.display());
                    let bytes = fs::read(&file).change_context(BeaconChainError)?;
                    let block =
                        rkyv::from_bytes::<'_, store::fragment::Slot<store::block::Block>>(&bytes)
                            .map_err(|_| BeaconChainError)
                            .attach_printable("failed to deserialize block")?;

                    segment_builder.add_block(block);
                }

                let segment_data = segment_builder
                    .to_segment_data()
                    .change_context(BeaconChainError)?;

                for segment in segment_data {
                    info!(
                        "writing segment {} ({:#})",
                        segment.name,
                        Byte::from_u64(segment.data.len() as u64)
                    );
                    fs::write(&out_dir.join(segment.name), &segment.data)
                        .change_context(BeaconChainError)
                        .attach_printable("failed to write segment")?;
                }

                Ok(())
            }
        }
    }
}

fn read_json(path: impl AsRef<Path>) -> Result<JsonBlock, BeaconChainError> {
    let path = path.as_ref();
    let file = fs::File::open(path).change_context(BeaconChainError)?;
    let mut reader = BufReader::new(file);
    let block: JsonBlock = serde_json::from_reader(&mut reader)
        .change_context(BeaconChainError)
        .attach_printable("failed to read JSON")
        .attach_printable_lazy(|| format!("path: {}", path.display()))?;

    Ok(block)
}
