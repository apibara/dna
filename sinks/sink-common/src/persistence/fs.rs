//! Persist indexer state to a directory.

use std::{
    fs,
    path::{Path, PathBuf},
};

use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::Result;
use tracing::info;

use crate::{SinkError, SinkErrorResultExt};

use super::common::PersistenceClient;

pub struct DirPersistence {
    path: PathBuf,
    sink_id: String,
}

impl DirPersistence {
    pub fn initialize(
        path: impl AsRef<Path>,
        sink_id: impl Into<String>,
    ) -> Result<Self, SinkError> {
        let path = path.as_ref();

        fs::create_dir_all(path)
            .persistence_client_error(&format!("failed to create directory {:?}", path))?;

        Ok(Self {
            path: path.into(),
            sink_id: sink_id.into(),
        })
    }

    pub fn cursor_file_path(&self) -> PathBuf {
        self.path.join(format!("{}.cursor", self.sink_id))
    }
}

#[async_trait]
impl PersistenceClient for DirPersistence {
    async fn lock(&mut self) -> Result<(), SinkError> {
        info!("Persistence to directory is not recommended for production usage.");
        Ok(())
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        Ok(())
    }

    async fn get_cursor(&mut self) -> Result<Option<Cursor>, SinkError> {
        let path = self.cursor_file_path();
        if path.exists() {
            let content = fs::read_to_string(&path)
                .persistence_client_error(&format!("failed to read cursor file {:?}", path))?;
            let cursor = serde_json::from_str(&content)
                .persistence_client_error("failed to deserialize cursor")?;
            Ok(Some(cursor))
        } else {
            Ok(None)
        }
    }

    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), SinkError> {
        let serialized = serde_json::to_string(&cursor)
            .persistence_client_error("failed to serialize cursor")?;
        let path = self.cursor_file_path();
        fs::write(&path, serialized)
            .persistence_client_error(&format!("failed to write cursor file {:?}", path))?;
        Ok(())
    }

    async fn delete_cursor(&mut self) -> Result<(), SinkError> {
        let path = self.cursor_file_path();
        fs::remove_file(&path)
            .persistence_client_error(&format!("failed to delete cursor file {:?}", path))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use apibara_core::node::v1alpha2::Cursor;
    use tempdir::TempDir;

    use super::DirPersistence;
    use crate::persistence::PersistenceClient;

    #[tokio::test]
    pub async fn test_get_put_delete_cursor() {
        let dir = TempDir::new("fs-persistence").unwrap();
        let sink_id = "test-sink".to_string();
        let mut persistence = DirPersistence::initialize(dir.path(), sink_id).unwrap();

        let cursor = persistence.get_cursor().await.unwrap();
        assert!(cursor.is_none());

        let new_cursor = Cursor {
            order_key: 123,
            unique_key: vec![1, 2, 3],
        };
        persistence.put_cursor(new_cursor.clone()).await.unwrap();

        let cursor = persistence.get_cursor().await.unwrap();
        assert_eq!(cursor, Some(new_cursor));

        persistence.delete_cursor().await.unwrap();
        let cursor = persistence.get_cursor().await.unwrap();
        assert!(cursor.is_none());
    }

    #[tokio::test]
    pub async fn test_lock_unlock() {
        let dir = TempDir::new("fs-persistence").unwrap();
        let sink_id = "test-sink".to_string();
        let mut persistence = DirPersistence::initialize(dir.path(), sink_id).unwrap();

        persistence.lock().await.unwrap();
        persistence.unlock().await.unwrap();
    }

    #[tokio::test]
    pub async fn test_multiple_indexers() {
        let dir = TempDir::new("fs-persistence").unwrap();
        let first_sink = "first-sink".to_string();
        let second_sink = "second-sink".to_string();
        let first_cursor = Cursor {
            order_key: 123,
            unique_key: vec![1, 2, 3],
        };
        let second_cursor = Cursor {
            order_key: 789,
            unique_key: vec![7, 8, 9],
        };

        let mut first = DirPersistence::initialize(dir.path(), first_sink).unwrap();
        let mut second = DirPersistence::initialize(dir.path(), second_sink).unwrap();

        first.put_cursor(first_cursor.clone()).await.unwrap();
        let cursor = second.get_cursor().await.unwrap();
        assert!(cursor.is_none());

        second.put_cursor(second_cursor.clone()).await.unwrap();
        let cursor = first.get_cursor().await.unwrap();
        assert_eq!(cursor, Some(first_cursor));

        first.delete_cursor().await.unwrap();
        let cursor = second.get_cursor().await.unwrap();
        assert_eq!(cursor, Some(second_cursor));

        second.delete_cursor().await.unwrap();
        let cursor = second.get_cursor().await.unwrap();
        assert!(cursor.is_none());
    }
}
