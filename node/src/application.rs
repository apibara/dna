pub use apibara_core::application::pb;
use async_trait::async_trait;

/// Application is responsible for handling input messages and generating an output sequence.
#[async_trait]
pub trait Application {
    /// Error type returned by fallible functions.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Called on the first start by the node. Returns the application configuration.
    async fn init(&self, request: pb::InitRequest) -> Result<pb::InitResponse, Self::Error>;
}
