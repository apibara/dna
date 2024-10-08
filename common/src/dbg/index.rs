use std::{path::PathBuf, time::Instant};

use byte_unit::Byte;
use clap::Subcommand;
use error_stack::{Result, ResultExt};
use tracing::info;

use crate::{
    fragment::{ArchivedIndexFragment, FragmentId, IndexGroupFragment},
    index::Index,
    segment::{ArchivedFragmentData, Segment},
};

use super::error::DebugCommandError;

#[derive(Subcommand, Debug)]
pub enum DebugIndexCommand {
    /// Dump the content of an index.
    TextDump {
        /// Path to the index file.
        path: PathBuf,
        /// Print the fragment at the given offset.
        #[arg(long)]
        offset: Option<usize>,
        /// Print the fragment with the given fragment ID.
        #[arg(long)]
        fragment_id: Option<FragmentId>,
    },
}

impl DebugIndexCommand {
    pub async fn run(self) -> Result<(), DebugCommandError> {
        match self {
            DebugIndexCommand::TextDump {
                path,
                offset,
                fragment_id,
            } => {
                info!(path = ?path, "reading index");

                let bytes = std::fs::read(&path)
                    .change_context(DebugCommandError)
                    .attach_printable("failed to read index file")?;

                let start = Instant::now();
                let segment = rkyv::access::<
                    rkyv::Archived<Segment<IndexGroupFragment>>,
                    rkyv::rancor::Error,
                >(&bytes)
                .change_context(DebugCommandError)
                .attach_printable("failed to deserialize index segment")?;
                let elapsed = start.elapsed();

                let first_block = rkyv::deserialize::<_, rkyv::rancor::Error>(&segment.first_block)
                    .change_context(DebugCommandError)
                    .attach_printable("failed to deserialize first block")?;

                info!(first_block = %first_block, time = ?elapsed, "segment read");

                if let Some(offset) = offset {
                    let data = segment
                        .data
                        .get(offset)
                        .ok_or(DebugCommandError)
                        .attach_printable("index at offset not found")?;
                    index_group_fragment_dump(offset, &fragment_id, data)?;
                } else {
                    for (offset, data) in segment.data.iter().enumerate() {
                        index_group_fragment_dump(offset, &fragment_id, data)?;
                    }
                }

                Ok(())
            }
        }
    }
}

fn index_group_fragment_dump(
    offset: usize,
    fragment_id: &Option<FragmentId>,
    data: &ArchivedFragmentData<IndexGroupFragment>,
) -> Result<(), DebugCommandError> {
    let cursor = rkyv::deserialize::<_, rkyv::rancor::Error>(&data.cursor)
        .change_context(DebugCommandError)
        .attach_printable("failed to deserialize cursor")?;
    info!(offset = offset, cursor = %cursor, "fragment data");

    if let Some(fragment_id) = fragment_id {
        let index = data
            .data
            .indexes
            .iter()
            .find(|f| f.fragment_id == *fragment_id)
            .ok_or(DebugCommandError)
            .attach_printable("fragment index not found")?;
        index_fragment_dump(index)?;
    } else {
        for index in data.data.indexes.iter() {
            index_fragment_dump(index)?;
        }
    }

    Ok(())
}

fn index_fragment_dump(fragment: &ArchivedIndexFragment) -> Result<(), DebugCommandError> {
    let start = Instant::now();
    let fragment = rkyv::deserialize::<_, rkyv::rancor::Error>(fragment)
        .change_context(DebugCommandError)
        .attach_printable("failed to deserialize fragment")?;
    let elapsed = start.elapsed();
    info!(
        fragment_id = fragment.fragment_id,
        range_start = fragment.range_start,
        range_len = fragment.range_len,
        indexes_len = fragment.indexes.len(),
        time = ?elapsed,
        "fragment index"
    );

    for index in fragment.indexes.iter() {
        match &index.index {
            Index::Bitmap(bitmap) => {
                let keys = bitmap.keys().collect::<Vec<_>>();
                let first = keys.first();
                let last = keys.last();
                let bitmap_size = bitmap.iter().map(|kv| kv.1.len() as u64).sum::<u64>();
                let bitmap_size = format!("{:#}", Byte::from_u64(bitmap_size));
                info!(
                    id = index.index_id,
                    keys = keys.len(),
                    bitmap_size,
                    first = ?first,
                    last = ?last,
                    "bitmap index"
                );
            }
        }
    }

    Ok(())
}
