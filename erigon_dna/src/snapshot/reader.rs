use std::path::{Path, PathBuf};

use reth_primitives::Header;
use tracing::{debug, trace};

use super::{
    file::SnapshotFileInfo,
    segment::{Segment, SegmentError},
};

pub struct SnapshotReader {
    segments: Option<SnapshotSegments>,
    base_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotReaderError {
    #[error(transparent)]
    SegmentError(#[from] SegmentError),
}

pub struct SnapshotSegments {
    headers: Vec<Segment<Header>>,
}

impl SnapshotReader {
    /// Creates a snapshot reader rooted in the given directory.
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            segments: None,
            base_dir,
        }
    }

    /// Reopens the snapshot files.
    pub fn reopen_snapshots<S: AsRef<str>>(
        &mut self,
        file_names: impl Iterator<Item = S>,
    ) -> Result<(), SnapshotReaderError> {
        let segment_files = file_names.map(|file_name| self.base_dir.join(file_name.as_ref()));
        let new_segments = if let Some(_segments) = self.segments.take() {
            todo!()
        } else {
            SnapshotSegments::new(segment_files)?
        };

        self.segments = Some(new_segments);

        Ok(())
    }

    /// Returns the header for the given block number.
    pub fn header_by_number(
        &self,
        block_number: u64,
    ) -> Result<Option<Header>, SnapshotReaderError> {
        if let Some(segments) = &self.segments {
            segments.header(block_number)
        } else {
            trace!(block_number = ?block_number, "no header segments");
            Ok(None)
        }
    }

    /// Returns the snapshot base dir.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl SnapshotSegments {
    pub fn new(segment_files: impl Iterator<Item = PathBuf>) -> Result<Self, SnapshotReaderError> {
        let mut segments = SnapshotSegments {
            headers: Vec::new(),
        };
        segments.reopen_snapshots(segment_files)?;
        Ok(segments)
    }

    pub fn reopen_snapshots(
        &mut self,
        segment_files: impl Iterator<Item = PathBuf>,
    ) -> Result<(), SnapshotReaderError> {
        debug!("reopen snapshots");
        let mut headers = Vec::new();
        for segment_file in segment_files {
            let info = SnapshotFileInfo::from_filename(&segment_file).unwrap();
            match info {
                SnapshotFileInfo::Headers(info) => {
                    let segment = Segment::<Header>::new(SnapshotFileInfo::Headers(info)).unwrap();
                    headers.push(segment);
                }
                SnapshotFileInfo::Bodies(info) => {
                    // TODO
                }
                SnapshotFileInfo::Transactions(info) => {
                    // TODO
                }
            }
        }
        self.headers = headers;

        Ok(())
    }

    pub fn header(&self, block_number: u64) -> Result<Option<Header>, SnapshotReaderError> {
        if let Some(seg) = self.headers.iter().find(|s| s.contains(block_number)) {
            let mut reader = seg.new_reader(1024 * 128);
            reader.reset(block_number)?;
            let block = reader.next().transpose()?;
            Ok(block)
        } else {
            trace!(block_number = ?block_number, "header outside of snapshot range");
            Ok(None)
        }
    }
}
