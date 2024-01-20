use std::{fmt::Display, path::PathBuf};

use async_trait::async_trait;
use byte_unit::Byte;
use error_stack::ResultExt;
use tracing::debug;

use crate::error::{DnaError, Result};

/// Format a size in bytes.
pub struct FormattedSize(pub usize);

impl Display for FormattedSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let byte = Byte::from_bytes(self.0 as u128).get_appropriate_unit(true);
        byte.fmt(f)
    }
}

/// Backend for storing and retrieving data.
#[async_trait]
pub trait StorageBackend {
    type Writer: StorageWriter;

    async fn writer(&mut self, prefix: &str) -> Result<Self::Writer>;
}

#[async_trait]
pub trait StorageWriter {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalStorageBackend {
    root: PathBuf,
}

pub struct LocalStorageWriter {
    root: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    type Writer = LocalStorageWriter;

    async fn writer(&mut self, prefix: &str) -> Result<Self::Writer> {
        let target_dir = self.root.join(prefix);
        tokio::fs::create_dir_all(&target_dir)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to create storage directory")
            .attach_printable_lazy(|| format!("dir: {:?}", target_dir))?;
        Ok(LocalStorageWriter::new(target_dir))
    }
}

impl LocalStorageWriter {
    fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

#[async_trait]
impl StorageWriter for LocalStorageWriter {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let target_path = self.root.join(key);
        debug!(target_path = ?target_path, "writing file to storage");
        tokio::fs::write(&target_path, value)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to write file")
            .attach_printable_lazy(|| format!("path: {:?}", target_path))?;
        Ok(())
    }
}
