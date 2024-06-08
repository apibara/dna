use std::path::PathBuf;

use async_trait::async_trait;
use error_stack::ResultExt;
use memmap2::Mmap;

use crate::error::{DnaError, Result};

use super::StorageBackend;

#[derive(Debug, Clone)]
pub struct LocalStorageBackend {
    root: PathBuf,
}

impl LocalStorageBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl LocalStorageBackend {
    fn target_dir(&self, prefix: &str) -> PathBuf {
        self.root.join(prefix)
    }

    fn target_file(&self, prefix: &str, filename: &str) -> PathBuf {
        let filename = format!("{}.segment", filename);
        self.target_dir(prefix).join(filename)
    }

    pub async fn prefix_exists(&mut self, prefix: impl AsRef<str> + Send) -> Result<bool> {
        let target_dir = self.root.join(prefix.as_ref());
        Ok(target_dir.exists())
    }

    pub fn mmap(&self, prefix: impl AsRef<str>, filename: impl AsRef<str> + Send) -> Result<Mmap> {
        let target_file = self.target_file(prefix.as_ref(), filename.as_ref());
        if !target_file.exists() {
            return Err(DnaError::Fatal)
                .attach_printable("storage file does not exist")
                .attach_printable_lazy(|| format!("file: {:?}", target_file));
        }

        let file = std::fs::File::open(&target_file)
            .change_context(DnaError::Io)
            .attach_printable("failed to open storage file for mmap")
            .attach_printable_lazy(|| format!("file: {:?}", target_file))?;

        let mmap = unsafe { Mmap::map(&file) }
            .change_context(DnaError::Io)
            .attach_printable("failed to create file mmap")?;

        Ok(mmap)
    }

    pub async fn remove(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<()> {
        let target_file = self.target_file(prefix.as_ref(), filename.as_ref());

        tokio::fs::remove_file(&target_file)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to delete storage file")
            .attach_printable_lazy(|| format!("file: {:?}", target_file))?;

        Ok(())
    }

    pub async fn remove_prefix(&mut self, prefix: impl AsRef<str> + Send) -> Result<()> {
        let target_dir = self.target_dir(prefix.as_ref());

        tokio::fs::remove_dir_all(&target_dir)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to delete storage directory")
            .attach_printable_lazy(|| format!("prefix: {:?}", prefix.as_ref()))
            .attach_printable_lazy(|| format!("dir: {:?}", target_dir))?;

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for LocalStorageBackend {
    type Reader = tokio::io::BufReader<tokio::fs::File>;
    type Writer = tokio::fs::File;

    async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool> {
        let filename = format!("{}.segment", filename.as_ref());

        let target_file = self.root.join(prefix.as_ref()).join(filename);
        Ok(target_file.exists())
    }

    async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Reader> {
        let target_file = self.target_file(prefix.as_ref(), filename.as_ref());
        if !target_file.exists() {
            return Err(DnaError::Fatal)
                .attach_printable("storage file does not exist")
                .attach_printable_lazy(|| format!("file: {:?}", target_file));
        }

        let file = tokio::fs::File::open(&target_file)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to open storage file")
            .attach_printable_lazy(|| format!("file: {:?}", target_file))?;

        let buffered = tokio::io::BufReader::new(file);
        Ok(buffered)
    }

    async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Writer> {
        let target_dir = self.target_dir(prefix.as_ref());
        tokio::fs::create_dir_all(&target_dir)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to create storage directory")
            .attach_printable_lazy(|| format!("dir: {:?}", target_dir))?;

        let target_file = self.target_file(prefix.as_ref(), filename.as_ref());
        let file = tokio::fs::File::create(&target_file)
            .await
            .change_context(DnaError::Io)
            .attach_printable("failed to create storage file")
            .attach_printable_lazy(|| format!("file: {:?}", target_file))?;

        Ok(file)
    }
}

#[cfg(test)]
mod tests {
    use error_stack::ResultExt;
    use tempdir::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::{
        error::{DnaError, Result},
        storage::StorageBackend,
    };

    use super::LocalStorageBackend;

    #[tokio::test]
    pub async fn test_local_storage_backend() -> Result<()> {
        let root = TempDir::new("test_local_storage_backend").change_context(DnaError::Fatal)?;
        let mut backend = LocalStorageBackend::new(root.path());

        assert!(backend.get("foo", "bar.txt").await.is_err());

        let mut writer = backend.put("foo", "bar.txt").await?;
        writer
            .write_all(b"hello world")
            .await
            .change_context(DnaError::Fatal)?;
        writer.shutdown().await.change_context(DnaError::Fatal)?;

        let mut reader = backend.get("foo", "bar.txt").await?;
        let mut buf = String::new();
        reader
            .read_to_string(&mut buf)
            .await
            .change_context(DnaError::Fatal)?;
        assert_eq!(buf, "hello world");
        Ok(())
    }
}
