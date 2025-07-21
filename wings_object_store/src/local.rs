//! Local file system object store factory implementation.
//!
//! This module provides a `LocalFileSystemFactory` that creates object stores
//! backed by the local file system. Each secret name gets its own subdirectory
//! within the configured root path.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::{Error as ObjectStoreError, ObjectStore, local::LocalFileSystem};
use tempfile::TempDir;

use wings_metadata_core::admin::SecretName;

use crate::ObjectStoreFactory;

/// Factory for creating local file system object stores.
///
/// This factory creates object store instances backed by the local file system.
/// Each secret name is mapped to a subdirectory within the root path, providing
/// isolation between different object store configurations.
///
///
pub struct LocalFileSystemFactory {
    root_path: PathBuf,
}

impl LocalFileSystemFactory {
    /// Create a new local file system factory with the given root path.
    ///
    /// # Arguments
    ///
    /// * `root_path` - The root directory where all object store data will be stored
    ///
    /// # Returns
    ///
    /// Returns a new `LocalFileSystemFactory` on success, or an `ObjectStoreError`
    /// if the path cannot be canonicalized.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The root path does not exist
    /// - The root path cannot be canonicalized
    /// - There are insufficient permissions to access the path
    pub fn new(root_path: impl AsRef<Path>) -> Result<Self, ObjectStoreError> {
        let canonical_path =
            std::fs::canonicalize(root_path.as_ref()).map_err(|e| ObjectStoreError::Generic {
                store: "LocalFileSystem",
                source: Box::new(e),
            })?;

        Ok(Self {
            root_path: canonical_path,
        })
    }

    /// Get the root path used by this factory.
    pub fn root_path(&self) -> &Path {
        &self.root_path
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for LocalFileSystemFactory {
    /// Create a local file system object store for the given secret name.
    ///
    /// The object store will use a subdirectory named after the secret's ID
    /// within the factory's root path. For example, if the root path is
    /// `/tmp/object-store` and the secret name is `my-bucket`, the object
    /// store will use `/tmp/object-store/my-bucket` as its root directory.
    ///
    /// # Arguments
    ///
    /// * `secret_name` - The secret name that identifies this object store instance
    ///
    /// # Returns
    ///
    /// Returns an `Arc<dyn ObjectStore>` backed by the local file system.
    ///
    /// # Errors
    ///
    /// This method will return an error if the local file system object store
    /// cannot be created with the computed path.
    async fn create_object_store(
        &self,
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        // Use the secret's ID to create a subdirectory path
        let store_path = self.root_path.join(secret_name.id());

        // Create the directory if it doesn't exist
        std::fs::create_dir_all(&store_path).map_err(|e| ObjectStoreError::Generic {
            store: "LocalFileSystem",
            source: Box::new(e),
        })?;

        // Create the LocalFileSystem with the specific prefix path
        let local_fs = LocalFileSystem::new_with_prefix(store_path)?;

        Ok(Arc::new(local_fs))
    }
}

/// Factory for creating temporary file system object stores.
///
/// This factory creates object store instances backed by a temporary directory
/// that is automatically cleaned up when the factory is dropped. This is ideal
/// for development, testing, and scenarios where you don't want to persist data.
///
/// Each secret name is mapped to a subdirectory within the temporary directory,
/// providing isolation between different object store configurations.
///
///
pub struct TemporaryFileSystemFactory {
    _temp_dir: TempDir,
    local_factory: LocalFileSystemFactory,
}

impl TemporaryFileSystemFactory {
    /// Create a new temporary file system factory.
    ///
    /// This creates a temporary directory that will be automatically cleaned up
    /// when the factory is dropped.
    ///
    /// # Returns
    ///
    /// Returns a new `TemporaryFileSystemFactory` on success, or an `ObjectStoreError`
    /// if the temporary directory cannot be created or the local factory cannot be
    /// initialized.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - The temporary directory cannot be created
    /// - The local file system factory cannot be initialized with the temp directory
    pub fn new() -> Result<Self, ObjectStoreError> {
        let temp_dir = TempDir::new().map_err(|e| ObjectStoreError::Generic {
            store: "TemporaryFileSystem",
            source: Box::new(e),
        })?;

        let local_factory = LocalFileSystemFactory::new(temp_dir.path())?;

        Ok(Self {
            _temp_dir: temp_dir,
            local_factory,
        })
    }

    /// Get the temporary root path used by this factory.
    ///
    /// Note: This path will be automatically cleaned up when the factory is dropped.
    pub fn root_path(&self) -> &Path {
        self.local_factory.root_path()
    }
}

#[async_trait::async_trait]
impl ObjectStoreFactory for TemporaryFileSystemFactory {
    /// Create a temporary file system object store for the given secret name.
    ///
    /// The object store will use a subdirectory named after the secret's ID
    /// within the temporary directory. The entire temporary directory tree
    /// will be automatically cleaned up when the factory is dropped.
    ///
    /// # Arguments
    ///
    /// * `secret_name` - The secret name that identifies this object store instance
    ///
    /// # Returns
    ///
    /// Returns an `Arc<dyn ObjectStore>` backed by a temporary file system.
    ///
    /// # Errors
    ///
    /// This method will return an error if the temporary file system object store
    /// cannot be created with the computed path.
    async fn create_object_store(
        &self,
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
        self.local_factory.create_object_store(secret_name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use wings_metadata_core::admin::SecretName;

    #[test]
    fn test_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        assert_eq!(factory.root_path(), temp_dir.path().canonicalize().unwrap());
    }

    #[test]
    fn test_factory_creation_invalid_path() {
        let result = LocalFileSystemFactory::new("/this/path/does/not/exist");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_object_store() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        let secret_name = SecretName::new("test-bucket").unwrap();
        let store = factory.create_object_store(secret_name).await.unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_multiple_object_stores() {
        let temp_dir = TempDir::new().unwrap();
        let factory = LocalFileSystemFactory::new(temp_dir.path()).unwrap();

        let secret1 = SecretName::new("bucket-1").unwrap();
        let secret2 = SecretName::new("bucket-2").unwrap();

        let store1 = factory.create_object_store(secret1).await.unwrap();
        let store2 = factory.create_object_store(secret2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));
    }

    #[test]
    fn test_temporary_factory_creation() {
        let factory = TemporaryFileSystemFactory::new().unwrap();

        // The root path should exist and be a valid directory
        assert!(factory.root_path().exists());
        assert!(factory.root_path().is_dir());
    }

    #[tokio::test]
    async fn test_temporary_factory_create_object_store() {
        let factory = TemporaryFileSystemFactory::new().unwrap();

        let secret_name = SecretName::new("temp-bucket").unwrap();
        let store = factory.create_object_store(secret_name).await.unwrap();

        // Verify that the store is created successfully
        assert!(store.as_ref() as *const _ as *const () != std::ptr::null());
    }

    #[tokio::test]
    async fn test_temporary_factory_cleanup() {
        let root_path = {
            let factory = TemporaryFileSystemFactory::new().unwrap();
            let secret_name = SecretName::new("cleanup-test").unwrap();
            let _store = factory.create_object_store(secret_name).await.unwrap();

            let path = factory.root_path().to_path_buf();
            assert!(path.exists());
            path
        }; // factory is dropped here

        // Give a moment for cleanup to occur
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // The temporary directory should be cleaned up
        assert!(!root_path.exists());
    }

    #[tokio::test]
    async fn test_temporary_factory_multiple_stores() {
        let factory = TemporaryFileSystemFactory::new().unwrap();

        let secret1 = SecretName::new("temp-bucket-1").unwrap();
        let secret2 = SecretName::new("temp-bucket-2").unwrap();

        let store1 = factory.create_object_store(secret1).await.unwrap();
        let store2 = factory.create_object_store(secret2).await.unwrap();

        // Both stores should be created successfully and be different instances
        assert!(!std::ptr::addr_eq(store1.as_ref(), store2.as_ref()));

        // Both should use the same root temporary directory
        let root_path = factory.root_path();
        assert!(root_path.join("temp-bucket-1").exists());
        assert!(root_path.join("temp-bucket-2").exists());
    }
}
