use std::sync::Arc;

use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::storage::StorageBackend;

use super::Snapshot;

static SNAPSHOT_FILENAME: &str = "snapshot";

#[async_trait]
pub trait SnapshotReader {
    /// Read the current snapshot, or the default snapshot if no snapshot exists.
    async fn read(&self) -> Result<Option<Snapshot>, SnapshotError>;
}

#[derive(Debug, Clone)]
pub struct SnapshotManager<S>
where
    S: StorageBackend + Send + Sync + 'static,
{
    storage: Arc<Mutex<S>>,
}

#[derive(Debug)]
pub enum SnapshotError {
    Io,
    Serialization,
}

impl<S> SnapshotManager<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage.into()),
        }
    }

    pub async fn read(&self) -> Result<Option<Snapshot>, SnapshotError> {
        let mut storage = self.storage.lock().await;

        if !storage
            .exists("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotError::Io)
            .attach_printable("failed to check if snapshot exists")?
        {
            return Ok(None);
        }

        let mut reader = storage
            .get("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotError::Io)
            .attach_printable("failed got get snapshot reader")?;

        let mut buf = String::new();
        reader
            .read_to_string(&mut buf)
            .await
            .change_context(SnapshotError::Io)
            .attach_printable("failed to read snapshot content")?;

        let snapshot = Snapshot::from_str(&buf)
            .change_context(SnapshotError::Serialization)
            .attach_printable("failed to deserialize snapshot")?;

        Ok(Some(snapshot))
    }

    pub async fn write(&self, snapshot: &Snapshot) -> Result<(), SnapshotError> {
        let snapshot = snapshot
            .to_vec()
            .change_context(SnapshotError::Serialization)
            .attach_printable("failed to serialize snapshot")?;

        let mut storage = self.storage.lock().await;

        let mut writer = storage
            .put("", SNAPSHOT_FILENAME)
            .await
            .change_context(SnapshotError::Io)
            .attach_printable("failed to get snapshot writer")?;

        writer
            .write_all(&snapshot)
            .await
            .change_context(SnapshotError::Io)
            .attach_printable("failed to write snapshot")?;

        writer.shutdown().await.change_context(SnapshotError::Io)?;

        Ok(())
    }
}

#[async_trait]
impl<S> SnapshotReader for SnapshotManager<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    async fn read(&self) -> Result<Option<Snapshot>, SnapshotError> {
        self.read().await
    }
}

impl error_stack::Context for SnapshotError {}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Io => write!(f, "IO error"),
            SnapshotError::Serialization => write!(f, "deserialization error"),
        }
    }
}
