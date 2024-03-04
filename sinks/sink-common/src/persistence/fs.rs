//! Persist indexer state to a directory.

use std::{
    fs,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use error_stack::Result;
use tracing::info;

use crate::{filter::Filter, SinkError, SinkErrorResultExt};

use super::common::{PersistedState, PersistenceClient};

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

        fs::create_dir_all(path).persistence(&format!("failed to create directory {:?}", path))?;

        Ok(Self {
            path: path.into(),
            sink_id: sink_id.into(),
        })
    }

    pub fn state_file_path(&self) -> PathBuf {
        self.path.join(format!("{}.state", self.sink_id))
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

    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        let path = self.state_file_path();
        if path.exists() {
            let content = fs::read_to_string(&path)
                .persistence(&format!("failed to read state file {:?}", path))?;
            let state =
                serde_json::from_str(&content).persistence("failed to deserialize state")?;
            Ok(state)
        } else {
            Ok(PersistedState::default())
        }
    }

    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        let serialized = serde_json::to_string(&state).persistence("failed to serialize state")?;
        let path = self.state_file_path();
        fs::write(&path, serialized)
            .persistence(&format!("failed to write state file {:?}", path))?;
        Ok(())
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        let path = self.state_file_path();
        fs::remove_file(&path).persistence(&format!("failed to delete state file {:?}", path))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /*
    use apibara_core::starknet::v1alpha2::Filter;
    use apibara_dna_protocol::dna::Cursor;
    use tempdir::TempDir;

    use super::DirPersistence;
    use crate::persistence::{common::PersistenceClient as PersistenceClientTrait, PersistedState};

    #[tokio::test]
    pub async fn test_get_put_delete_state() {
        let dir = TempDir::new("fs-persistence").unwrap();
        let sink_id = "test-sink".to_string();
        let mut persistence = DirPersistence::initialize(dir.path(), sink_id).unwrap();

        let state = persistence.get_state::<Filter>().await.unwrap();
        assert!(state.cursor.is_none());

        let new_cursor = Cursor {
            order_key: 123,
            unique_key: vec![1, 2, 3],
        };
        let new_state = PersistedState::<Filter>::new(Some(new_cursor.clone()), None);

        persistence.put_state(new_state).await.unwrap();

        let state = persistence.get_state::<Filter>().await.unwrap();
        assert_eq!(state.cursor, Some(new_cursor));

        persistence.delete_state().await.unwrap();
        let state = persistence.get_state::<Filter>().await.unwrap();
        assert!(state.cursor.is_none());
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
        let first_state = PersistedState::<Filter>::with_cursor(first_cursor.clone());

        let second_cursor = Cursor {
            order_key: 789,
            unique_key: vec![7, 8, 9],
        };
        let second_state = PersistedState::<Filter>::with_cursor(second_cursor.clone());

        let mut first = DirPersistence::initialize(dir.path(), first_sink).unwrap();
        let mut second = DirPersistence::initialize(dir.path(), second_sink).unwrap();

        first.put_state(first_state).await.unwrap();
        let state = second.get_state::<Filter>().await.unwrap();
        assert!(state.cursor.is_none());

        second.put_state(second_state).await.unwrap();
        let state = first.get_state::<Filter>().await.unwrap();
        assert_eq!(state.cursor, Some(first_cursor));

        first.delete_state().await.unwrap();
        let state = second.get_state::<Filter>().await.unwrap();
        assert_eq!(state.cursor, Some(second_cursor));

        second.delete_state().await.unwrap();
        let state = second.get_state::<Filter>().await.unwrap();
        assert!(state.cursor.is_none());
    }
    */
}
