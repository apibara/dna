//! Snapshot file info.

use std::path::{Path, PathBuf};

use super::error::SnapshotError;

/// Information about a snapshot file.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub version: u8,
    pub from_block: u64,
    pub to_block: u64,
    pub path: PathBuf,
}

/// Snapshot file.
#[derive(Debug, Clone)]
pub enum SnapshotFileInfo {
    Headers(SnapshotInfo),
    Bodies(SnapshotInfo),
    Transactions(SnapshotInfo),
}

impl SnapshotFileInfo {
    pub fn from_filename(file_path: &Path) -> Result<Self, SnapshotError> {
        let file_name = file_path
            .file_name()
            .expect("missing file name")
            .to_str()
            .expect("failde to convert str")
            .split('.')
            .collect::<Vec<_>>();

        if file_name.len() != 2 {
            panic!("invalid format. no extension")
        }

        let parts = file_name[0].split('-').collect::<Vec<_>>();
        if parts.len() < 4 {
            panic!("invalid format. expected v1-001500-002000-bodies.seg")
        }

        if parts[0] != "v1" {
            panic!("invalid version")
        }

        let from_block = parts[1].parse::<u64>().expect("invalid from block");
        let to_block = parts[2].parse::<u64>().expect("invalid to block");

        let info = SnapshotInfo {
            version: 1,
            from_block: from_block * 1_000,
            to_block: to_block * 1_000,
            path: file_path.to_path_buf(),
        };

        match parts[3] {
            "headers" => Ok(SnapshotFileInfo::Headers(info)),
            "bodies" => Ok(SnapshotFileInfo::Bodies(info)),
            "transactions" => Ok(SnapshotFileInfo::Transactions(info)),
            _ => panic!("invalid file type"),
        }
    }

    pub fn info(&self) -> &SnapshotInfo {
        match self {
            SnapshotFileInfo::Headers(info) => info,
            SnapshotFileInfo::Bodies(info) => info,
            SnapshotFileInfo::Transactions(info) => info,
        }
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
            info.info().path.to_str().expect("failed to convert str"),
            path
        );
    }
}
