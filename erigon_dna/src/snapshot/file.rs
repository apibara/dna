//! Snapshot file info.

use std::path::{Path, PathBuf};

/// Information about a snapshot file.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// File version.
    pub version: u8,
    /// Starting block.
    pub from_block: u64,
    /// Ending block.
    pub to_block: u64,
    /// Directory containing the snapshot files.
    pub dir_path: PathBuf,
}

/// Error related to [SnapshotInfo].
#[derive(Debug, thiserror::Error)]
pub enum SnapshotInfoError {
    #[error("filename is invalid or missing")]
    InvalidFilename,
    #[error("filename is expected to be in the format v1-001500-002000-bodies.seg")]
    MalformedFilename,
    #[error("file version is not supported. Supported versions: v1")]
    UnsupportedVersion,
}

/// Snapshot file.
#[derive(Debug, Clone)]
pub enum SnapshotFileInfo {
    Headers(SnapshotInfo),
    Bodies(SnapshotInfo),
    Transactions(SnapshotInfo),
}

impl SnapshotFileInfo {
    /// Create snapshot information from its filename.
    pub fn from_filename(file_path: &Path) -> Result<Self, SnapshotInfoError> {
        let dir_path = file_path
            .parent()
            .ok_or(SnapshotInfoError::InvalidFilename)?;

        let file_name = file_path
            .file_name()
            .ok_or(SnapshotInfoError::InvalidFilename)?
            .to_str()
            .ok_or(SnapshotInfoError::InvalidFilename)?
            .split('.')
            .collect::<Vec<_>>();

        if file_name.len() != 2 {
            return Err(SnapshotInfoError::MalformedFilename);
        }

        let parts = file_name[0].split('-').collect::<Vec<_>>();
        if parts.len() < 4 {
            return Err(SnapshotInfoError::MalformedFilename);
        }

        if parts[0] != "v1" {
            return Err(SnapshotInfoError::UnsupportedVersion);
        }

        let from_block = parts[1]
            .parse::<u64>()
            .map_err(|_| SnapshotInfoError::MalformedFilename)?;
        let to_block = parts[2]
            .parse::<u64>()
            .map_err(|_| SnapshotInfoError::MalformedFilename)?;

        let info = SnapshotInfo {
            version: 1,
            from_block: from_block * 1_000,
            to_block: to_block * 1_000,
            dir_path: dir_path.to_path_buf(),
        };

        match parts[3] {
            "headers" => Ok(SnapshotFileInfo::Headers(info)),
            "bodies" => Ok(SnapshotFileInfo::Bodies(info)),
            "transactions" => Ok(SnapshotFileInfo::Transactions(info)),
            _ => Err(SnapshotInfoError::MalformedFilename),
        }
    }

    /// Returns the inner file information about a file.
    pub fn info(&self) -> &SnapshotInfo {
        match self {
            SnapshotFileInfo::Headers(info) => info,
            SnapshotFileInfo::Bodies(info) => info,
            SnapshotFileInfo::Transactions(info) => info,
        }
    }

    /// Returns the path to the segment file.
    pub fn segment_path(&self) -> PathBuf {
        self.base_path().with_extension("seg")
    }

    /// Returns the path to the index file.
    pub fn index_path(&self) -> PathBuf {
        self.base_path().with_extension("idx")
    }

    fn base_path(&self) -> PathBuf {
        let info = self.info();
        let type_name = match self {
            SnapshotFileInfo::Headers(_) => "headers",
            SnapshotFileInfo::Bodies(_) => "bodies",
            SnapshotFileInfo::Transactions(_) => "transactions",
        };
        let from_block = info.from_block / 1_000;
        let to_block = info.to_block / 1_000;
        let filename = format!(
            "v{}-{:0>6}-{:0>6}-{}",
            info.version, from_block, to_block, type_name
        );
        info.dir_path.join(filename)
    }
}

impl SnapshotInfo {
    /// Creates a new snapshot info.
    pub fn new(version: u8, from_block: u64, to_block: u64, dir_path: &Path) -> Self {
        Self {
            version,
            from_block,
            to_block,
            dir_path: dir_path.to_path_buf(),
        }
    }

    /// Returns the headers file info.
    pub fn headers(&self) -> SnapshotFileInfo {
        SnapshotFileInfo::Headers(self.clone())
    }

    /// Returns the bodies file info.
    pub fn bodies(&self) -> SnapshotFileInfo {
        SnapshotFileInfo::Bodies(self.clone())
    }

    /// Returns the transactions file info.
    pub fn transactions(&self) -> SnapshotFileInfo {
        SnapshotFileInfo::Transactions(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::SnapshotFileInfo;

    #[test]
    pub fn test_snapshot_file_info() {
        let path = "/var/data/erigon/snapshots/v1-001500-002000-bodies.seg";
        let info = SnapshotFileInfo::from_filename(path.as_ref()).expect("failed to parse");
        assert_eq!(info.info().version, 1);
        assert_eq!(info.info().from_block, 1_500_000);
        assert_eq!(info.info().to_block, 2_000_000);
        assert_eq!(
            info.info()
                .dir_path
                .to_str()
                .expect("failed to convert str"),
            "/var/data/erigon/snapshots"
        );
    }

    #[test]
    pub fn test_bodies_snapshot_file() {
        let path = "/var/data/erigon/snapshots/v1-001500-002000-bodies.seg";
        let info = SnapshotFileInfo::from_filename(path.as_ref()).expect("failed to parse");
        assert_eq!(
            info.segment_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-bodies.seg"
        );
        assert_eq!(
            info.index_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-bodies.idx"
        );
    }

    #[test]
    pub fn test_headers_snapshot_file() {
        let path = "/var/data/erigon/snapshots/v1-001500-002000-headers.seg";
        let info = SnapshotFileInfo::from_filename(path.as_ref()).expect("failed to parse");
        assert_eq!(
            info.segment_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-headers.seg"
        );
        assert_eq!(
            info.index_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-headers.idx"
        );
    }

    #[test]
    pub fn test_transactions_snapshot_file() {
        let path = "/var/data/erigon/snapshots/v1-001500-002000-transactions.seg";
        let info = SnapshotFileInfo::from_filename(path.as_ref()).expect("failed to parse");
        assert_eq!(
            info.segment_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-transactions.seg"
        );
        assert_eq!(
            info.index_path().to_str().expect("failed to convert str"),
            "/var/data/erigon/snapshots/v1-001500-002000-transactions.idx"
        );
    }
}
