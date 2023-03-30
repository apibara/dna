//! Read a snapshot segment.
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

/// Represents a single segment.
#[derive(Debug)]
pub struct Segment<T> {
    decompressor: Decompressor,
    _phantom: PhantomData<T>,
}

/// Error related to [Segment].
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {}

impl<T> Segment<T> {
    pub fn from_info(info: SnapshotFileInfo) -> Self {
        let decompressor =
            Decompressor::new(info.segment_path()).expect("failed to open decompressor");
        Self {
            decompressor,
            _phantom: PhantomData::default(),
        }
    }

    pub fn decompressor(&self) -> &Decompressor {
        &self.decompressor
    }
}
