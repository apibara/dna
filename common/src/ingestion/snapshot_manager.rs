use error_stack::{Result, ResultExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::storage::StorageBackend;

use super::Snapshot;

static SNAPSHOT_FILENAME: &str = "snapshot";

pub struct SnapshotManager<S>
where
    S: StorageBackend + Send + Sync + 'static,
{
    storage: S,
}

#[derive(Debug)]
pub enum SnapshotManagerError {
    Io,
    Serialization,
}

impl<S> SnapshotManager<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub async fn read(&mut self) -> Result<Option<Snapshot>, SnapshotManagerError> {
        if !self
            .storage
            .exists("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotManagerError::Io)
            .attach_printable("failed to check if snapshot exists")?
        {
            return Ok(None);
        }

        let mut reader = self
            .storage
            .get("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotManagerError::Io)
            .attach_printable("failed got get snapshot reader")?;

        let mut buf = String::new();
        reader
            .read_to_string(&mut buf)
            .await
            .change_context(SnapshotManagerError::Io)
            .attach_printable("failed to read snapshot content")?;

        let snapshot = Snapshot::from_str(&buf)
            .change_context(SnapshotManagerError::Serialization)
            .attach_printable("failed to deserialize snapshot")?;

        Ok(Some(snapshot))
    }

    pub async fn write(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotManagerError> {
        let snapshot = snapshot
            .to_vec()
            .change_context(SnapshotManagerError::Serialization)
            .attach_printable("failed to serialize snapshot")?;

        let mut writer = self
            .storage
            .put("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotManagerError::Io)
            .attach_printable("failed to get snapshot writer")?;

        writer
            .write_all(&snapshot)
            .await
            .change_context(SnapshotManagerError::Io)
            .attach_printable("failed to write snapshot")?;

        writer
            .shutdown()
            .await
            .change_context(SnapshotManagerError::Io)?;

        Ok(())
    }
}
impl error_stack::Context for SnapshotManagerError {}

impl std::fmt::Display for SnapshotManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotManagerError::Io => write!(f, "IO error"),
            SnapshotManagerError::Serialization => write!(f, "deserialization error"),
        }
    }
}
