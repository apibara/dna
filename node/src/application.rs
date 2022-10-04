pub use apibara_core::application::pb;
use async_trait::async_trait;

/// Application is responsible for handling input messages and generating an output sequence.
#[async_trait]
pub trait Application {
    /// Error type returned by fallible functions.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Called on the first start. Returns the application configuration.
    async fn init(&mut self, request: pb::InitRequest) -> Result<pb::InitResponse, Self::Error>;

    /// Called when the application receives data from an input stream.
    async fn receive_data(
        &mut self,
        request: pb::ReceiveDataRequest,
    ) -> Result<pb::ReceiveDataResponse, Self::Error>;
}
