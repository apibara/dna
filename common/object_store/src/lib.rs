//! Object store factory for creating ObjectStore instances from runtime configuration.
//!
//! This module provides the `ObjectStoreFactory` trait that allows components to create
//! `ObjectStore` clients dynamically based on secret configurations loaded at runtime.
//! The factory abstracts away the details of different object store implementations
//! (S3, Azure, GCS, etc.) and provides a unified interface for object store creation.

pub mod local;

use std::sync::Arc;

use object_store::ObjectStore;
use wings_metadata_core::admin::SecretName;

pub use local::{LocalFileSystemFactory, TemporaryFileSystemFactory};

/// Factory trait for creating ObjectStore instances from secret configurations.
///
/// This trait allows components to create object store clients dynamically
/// based on configurations referenced by secret names. The actual secret
/// resolution and object store instantiation is left to the implementor.
///

#[async_trait::async_trait]
pub trait ObjectStoreFactory: Send + Sync {
    /// Create an ObjectStore instance from the configuration referenced by the secret name.
    ///
    /// # Arguments
    ///
    /// * `secret_name` - The name of the secret containing the object store configuration
    ///
    /// # Returns
    ///
    /// Returns an `Arc<dyn ObjectStore>` on success, or an `object_store::Error` if the
    /// secret cannot be resolved or the object store cannot be created.
    ///
    /// # Errors
    ///
    /// This method can return errors for various reasons:
    /// - The secret name does not exist
    /// - The secret configuration is invalid
    /// - The object store cannot be instantiated (e.g., invalid credentials)
    /// - Network connectivity issues during object store initialization
    async fn create_object_store(
        &self,
        secret_name: SecretName,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error>;
}
