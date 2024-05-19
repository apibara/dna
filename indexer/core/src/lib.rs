use std::fmt;

use error_stack::Result;
use serde_json::Value;

#[derive(Debug)]
pub struct KeyValueStorageError;

/// A context for the indexer.
pub struct Context {
    /// The current block number.
    pub block_number: u64,
}

/// A key-value storage interface.
pub trait KeyValueStorage {
    /// Get the most recent value for the given key.
    fn get(
        &mut self,
        ctx: &Context,
        key: impl AsRef<str>,
    ) -> Result<Option<Value>, KeyValueStorageError>;

    /// Set the value for the given key.
    fn set(
        &mut self,
        ctx: &Context,
        key: impl AsRef<str>,
        value: &Value,
    ) -> Result<(), KeyValueStorageError>;

    /// Delete the value for the given key.
    fn del(&mut self, ctx: &Context, key: impl AsRef<str>) -> Result<(), KeyValueStorageError>;

    /// Invalidate values inserted at or after `block_number`.
    fn invalidate(&mut self, block_number: u64) -> Result<(), KeyValueStorageError>;

    /// Remove finalized data.
    fn remove_finalized(&mut self, block_number: u64) -> Result<(), KeyValueStorageError>;
}

impl error_stack::Context for KeyValueStorageError {}

impl fmt::Display for KeyValueStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "key-value storage error")
    }
}
