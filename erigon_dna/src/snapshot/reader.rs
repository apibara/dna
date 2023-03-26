//! Iterate over erigon snapshots.
//!
//! Roughly modelled after `block_snapshots.go`

use std::{
    marker::PhantomData,
    ops::Range,
    path::{Path, PathBuf},
};

use tracing::{info, trace};

use super::{
    decompress::Decompressor,
    error::SnapshotError,
    file::{SnapshotFileInfo, SnapshotInfo},
};

pub type Header = ();
pub type Body = ();
pub type Transaction = ();

/// Provide read-only access over erigon snapshots.
pub struct RoSnapshots {
    root_dir: PathBuf,
    headers: Vec<Segment<Header>>,
    bodies: Vec<Segment<Body>>,
    transactions: Vec<Segment<Transaction>>,
}

/// Represents a single segment.
#[derive(Debug)]
pub struct Segment<T> {
    decompressor: Decompressor,
    range: Range<u64>,
    _phantom: PhantomData<T>,
}

impl RoSnapshots {
    pub fn new(root_dir: &Path) -> Self {
        RoSnapshots {
            root_dir: root_dir.into(),
            headers: Vec::new(),
            bodies: Vec::new(),
            transactions: Vec::new(),
        }
    }

    pub fn reopen_files(&mut self, files: &[String]) -> Result<(), SnapshotError> {
        let mut new_headers = Vec::new();
        let mut new_bodies = Vec::new();
        let mut new_transactions = Vec::new();
        for file in files {
            let file_path = self.root_dir.join(file);
            match SnapshotFileInfo::from_filename(&file_path)? {
                SnapshotFileInfo::Headers(info) => {
                    trace!(info = ?info, "reopening headers");
                    let segment = Segment::from_info(info);
                    new_headers.push(segment);
                }
                SnapshotFileInfo::Bodies(info) => {
                    trace!(info = ?info, "reopening bodies");
                    let segment = Segment::from_info(info);
                    new_bodies.push(segment);
                }
                SnapshotFileInfo::Transactions(info) => {
                    trace!(info = ?info, "reopening transactions");
                    let segment = Segment::from_info(info);
                    new_transactions.push(segment);
                }
            }
        }

        self.headers = new_headers;
        self.bodies = new_bodies;
        self.transactions = new_transactions;

        Ok(())
    }

    pub fn view_headers(&self, block_number: u64) -> Option<&Segment<Header>> {
        // TODO: check if block number in range.
        // TODO: check if indexes loaded.
        self.headers
            .iter()
            .find(|segment| segment.range.contains(&block_number))
    }

    pub fn view_bodies(&self, block_number: u64) -> Option<&Segment<Body>> {
        // TODO: check if block number in range.
        // TODO: check if indexes loaded.
        self.bodies
            .iter()
            .find(|segment| segment.range.contains(&block_number))
    }

    pub fn view_transactions(&self, block_number: u64) -> Option<&Segment<Transaction>> {
        // TODO: check if block number in range.
        // TODO: check if indexes loaded.
        self.transactions
            .iter()
            .find(|segment| segment.range.contains(&block_number))
    }
}

impl<T> Segment<T> {
    pub fn from_info(info: SnapshotInfo) -> Self {
        let decompressor = Decompressor::new(info.path).expect("failed to open decompressor");
        let range = info.from_block..info.to_block;
        Self {
            decompressor,
            range,
            _phantom: PhantomData::default(),
        }
    }

    pub fn decompressor(&self) -> &Decompressor {
        &self.decompressor
    }
}
