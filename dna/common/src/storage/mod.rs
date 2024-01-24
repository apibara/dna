use std::fmt::Display;

use async_trait::async_trait;
use byte_unit::Byte;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Result;

mod local;

pub use self::local::LocalStorageBackend;

#[async_trait]
pub trait StorageBackend {
    type Reader: AsyncRead;
    type Writer: AsyncWrite + Unpin;

    /// Returns true if `filename` exists in `prefix`.
    async fn exists(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<bool>;

    /// Returns a [Self::Reader] for `filename` in `prefix`.
    async fn get(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Reader>;

    /// Returns a [Self::Writer] for `filename` in `prefix`.
    async fn put(
        &mut self,
        prefix: impl AsRef<str> + Send,
        filename: impl AsRef<str> + Send,
    ) -> Result<Self::Writer>;
}

/// Format a size in bytes.
pub struct FormattedSize(pub usize);

impl Display for FormattedSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let byte = Byte::from_bytes(self.0 as u128).get_appropriate_unit(true);
        byte.fmt(f)
    }
}
