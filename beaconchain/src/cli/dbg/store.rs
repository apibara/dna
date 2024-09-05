use std::{fs, path::PathBuf};

use byte_unit::Byte;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{error::BeaconChainError, segment::SegmentBuilder, store};

#[derive(Subcommand, Debug)]
pub enum DebugStoreCommand {
    /// Create a segment from indexed blocks.
    CreateSegment {
        /// Files with the indexed blocks.
        files: Vec<PathBuf>,
        /// Path to the output directory.
        #[arg(long)]
        out_dir: PathBuf,
    },
}

impl DebugStoreCommand {
    pub async fn run(self) -> Result<(), BeaconChainError> {
        match self {
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
                        .or_else(|err| {
                            Err(BeaconChainError)
                                .attach_printable("failed to deserialize block")
                                .attach_printable_lazy(|| format!("error: {}", err))
                        })?;

                let first_block = block.cursor();
                let mut segment_builder = SegmentBuilder::new(&first_block);
                segment_builder.add_block(block);

                for file in files {
                    info!("reading block from {}", file.display());
                    let bytes = fs::read(&file).change_context(BeaconChainError)?;
                    let block =
                        rkyv::from_bytes::<'_, store::fragment::Slot<store::block::Block>>(&bytes)
                            .or_else(|err| {
                                Err(BeaconChainError)
                                    .attach_printable("failed to deserialize block")
                                    .attach_printable_lazy(|| format!("error: {}", err))
                            })?;

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
                    fs::write(out_dir.join(segment.name), &segment.data)
                        .change_context(BeaconChainError)
                        .attach_printable("failed to write segment")?;
                }

                Ok(())
            }
        }
    }
}
