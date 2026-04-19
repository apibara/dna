use std::path::PathBuf;

use bytes::{Buf, BufMut, BytesMut};
use clap::Subcommand;
use error_stack::{Result, ResultExt};

use crate::error::StarknetError;
use apibara_dna_common::chain::{CanonicalChainBuilder, CanonicalChainSegment};
use apibara_dna_common::Cursor;

#[derive(Subcommand, Debug)]
pub enum CanonCommand {
    /// Inspect a canonical chain segment
    Inspect {
        /// Path to the canonical chain segment file
        path: PathBuf,
    },
    /// Recover from a deep reorg.
    Recover {
        /// Path to the completed chain segment file (starting with 'z-')
        completed: PathBuf,
        /// Path to the recent chain segment file
        recent: PathBuf,
        /// New head cursor in format: block_number:block_hash
        head: String,
        /// Output file path for the recovered segment
        #[arg(long)]
        out: PathBuf,
    },
}

impl CanonCommand {
    pub async fn run(self) -> Result<(), StarknetError> {
        match self {
            CanonCommand::Inspect { path } => {
                let segment = read_segment_from_path(&path).await?;

                println!("Canonical Chain Segment");
                println!("========================");

                if let Some(prev) = &segment.previous_segment {
                    println!("Previous Segment:");
                    println!(
                        "  First block: {} ({})",
                        prev.first_block.number, prev.first_block.hash
                    );
                    println!(
                        "  Last block:  {} ({})",
                        prev.last_block.number, prev.last_block.hash
                    );
                } else {
                    println!("Previous Segment: None");
                }

                println!();
                println!("Segment Info:");
                println!(
                    "  First block: {} ({})",
                    segment.info.first_block.number, segment.info.first_block.hash
                );
                println!(
                    "  Last block:  {} ({})",
                    segment.info.last_block.number, segment.info.last_block.hash
                );
                println!("  Total blocks: {}", segment.canonical.len());

                println!();
                println!("Canonical Blocks:");
                for (i, block) in segment.canonical.iter().enumerate() {
                    let block_number = segment.info.first_block.number + i as u64;
                    println!("  Block {}: {}", block_number, block.hash);
                    if !block.reorgs.is_empty() {
                        println!("    Reorgs: {} alternative(s)", block.reorgs.len());
                        for (hash, target) in block.reorgs.iter() {
                            println!(
                                "      - {} -> target {} ({})",
                                hash, target.number, target.hash
                            );
                        }
                    }
                }

                if !segment.extra_reorgs.is_empty() {
                    println!();
                    println!("Extra Reorgs (blocks shrunk while offline):");
                    for extra in segment.extra_reorgs.iter() {
                        println!(
                            "  Block {}: {} alternative(s)",
                            extra.block_number,
                            extra.reorgs.len()
                        );
                        for (hash, target) in extra.reorgs.iter() {
                            println!(
                                "    - {} -> target {} ({})",
                                hash, target.number, target.hash
                            );
                        }
                    }
                }

                Ok(())
            }
            CanonCommand::Recover {
                completed,
                recent,
                head,
                out,
            } => {
                // Parse the new head cursor
                let new_head = parse_cursor(&head)
                    .change_context(StarknetError)
                    .attach_printable_lazy(|| format!("invalid head cursor format: {}", head))?;

                // Read both segments
                let completed_segment = read_segment_from_path(&completed).await?;
                let recent_segment = read_segment_from_path(&recent).await?;

                // Clone info needed for validation before moving completed_segment
                let completed_first = completed_segment.info.first_block.number;
                let completed_last = completed_segment.info.last_block.number;
                let completed_last_hash = completed_segment.info.last_block.hash.clone();

                // Verify the new head is in the first (completed) segment
                if new_head.number < completed_first || new_head.number > completed_last {
                    return Err(StarknetError)
                        .attach_printable("new head cursor must be in the completed segment")
                        .attach_printable_lazy(|| {
                            format!(
                                "completed segment range: {} - {}",
                                completed_first, completed_last
                            )
                        })
                        .attach_printable_lazy(|| format!("new head: {}", new_head.number));
                }

                // Create builder from completed segment
                let mut builder = CanonicalChainBuilder::restore_from_segment(completed_segment)
                    .change_context(StarknetError)
                    .attach_printable("failed to restore from completed segment")?;

                // Grow the chain with content from the recent segment
                // We need to convert the recent segment's canonical blocks to BlockInfo and grow
                for (i, block) in recent_segment.canonical.iter().enumerate() {
                    let block_number = recent_segment.info.first_block.number + i as u64;
                    let block_info = apibara_dna_common::chain::BlockInfo {
                        number: block_number,
                        hash: block.hash.clone(),
                        parent: if i == 0 {
                            // For the first block in recent, parent is the last block of completed
                            completed_last_hash.clone()
                        } else {
                            recent_segment.canonical[i - 1].hash.clone()
                        },
                    };
                    builder
                        .grow(block_info)
                        .change_context(StarknetError)
                        .attach_printable_lazy(|| {
                            format!("failed to grow at block {}", block_number)
                        })?;
                }

                // Shrink to the new head
                builder
                    .shrink(new_head.clone())
                    .change_context(StarknetError)
                    .attach_printable("failed to shrink to new head")?;

                // Get the final segment
                let final_segment = builder
                    .current_segment()
                    .change_context(StarknetError)
                    .attach_printable("failed to get final segment")?;

                // Write the segment to the output file
                write_segment_to_path(&out, &final_segment).await?;

                println!(
                    "Successfully recovered chain segment. Output written to: {}",
                    out.display()
                );
                println!("New head: {}", new_head);
                println!("Segment info:");
                println!(
                    "  First block: {} ({})",
                    final_segment.info.first_block.number, final_segment.info.first_block.hash
                );
                println!(
                    "  Last block:  {} ({})",
                    final_segment.info.last_block.number, final_segment.info.last_block.hash
                );
                println!("  Total blocks: {}", final_segment.canonical.len());

                Ok(())
            }
        }
    }
}

/// Parse a cursor from string format: "block_number:block_hash"
fn parse_cursor(s: &str) -> Result<Cursor, StarknetError> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(StarknetError).attach_printable("expected format: block_number:block_hash");
    }

    let number: u64 = parts[0]
        .parse::<u64>()
        .change_context(StarknetError)
        .attach_printable("failed to parse block number")?;

    let hash_hex = parts[1];
    let hash_bytes = if let Some(stripped) = hash_hex.strip_prefix("0x") {
        hex::decode(stripped)
    } else {
        hex::decode(hash_hex)
    }
    .change_context(StarknetError)
    .attach_printable("failed to decode hash hex")?;

    Ok(Cursor {
        number,
        hash: apibara_dna_common::Hash(hash_bytes),
    })
}

async fn read_segment_from_path(path: &PathBuf) -> Result<CanonicalChainSegment, StarknetError> {
    let compressed = tokio::fs::read(path)
        .await
        .change_context(StarknetError)
        .attach_printable_lazy(|| format!("failed to read file: {}", path.display()))?;

    // Decompress the data into BytesMut
    let decompressed = bytes::BytesMut::with_capacity(compressed.len() * 2);
    let mut writer = decompressed.writer();
    zstd::stream::copy_decode(&compressed[..], &mut writer)
        .change_context(StarknetError)
        .attach_printable("failed to decompress file")?;
    let decompressed = writer.into_inner();

    // Extract checksum (last 4 bytes)
    let checksum = (&decompressed[decompressed.len() - 4..]).get_u32();
    let data = &decompressed[..decompressed.len() - 4];

    // Verify checksum
    if crc32fast::hash(data) != checksum {
        return Err(StarknetError).attach_printable("checksum mismatch");
    }

    let segment: CanonicalChainSegment = rkyv::from_bytes::<_, rkyv::rancor::Error>(data)
        .change_context(StarknetError)
        .attach_printable("failed to deserialize segment")?;
    Ok(segment)
}

async fn write_segment_to_path(
    path: &PathBuf,
    segment: &CanonicalChainSegment,
) -> Result<(), StarknetError> {
    // Serialize the segment
    let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(segment)
        .change_context(StarknetError)
        .attach_printable("failed to serialize segment")?;

    // Compute checksum and append it
    let checksum = crc32fast::hash(&serialized);
    let mut data_with_checksum = BytesMut::with_capacity(serialized.len() + 4);
    data_with_checksum.extend_from_slice(&serialized);
    data_with_checksum.put_u32(checksum);

    // Compress the data
    let mut compressed = Vec::with_capacity(data_with_checksum.len());
    let mut writer = std::io::Cursor::new(&mut compressed);
    zstd::stream::copy_encode(data_with_checksum.reader(), &mut writer, 0)
        .change_context(StarknetError)
        .attach_printable("failed to compress data")?;

    // Write to file
    tokio::fs::write(path, compressed)
        .await
        .change_context(StarknetError)
        .attach_printable_lazy(|| format!("failed to write file: {}", path.display()))?;

    Ok(())
}
