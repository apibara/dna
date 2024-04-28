use std::time::Duration;

use error_stack::{Result, ResultExt};
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, Uri},
};

use crate::dna::stream::dna_stream_client::DnaStreamClient;

use super::{client::StreamClient, MetadataInterceptor, StreamClientError};

///! A builder for the DNA stream client.
pub struct StreamClientBuilder {
    token: Option<String>,
    max_message_size: Option<usize>,
    metadata: MetadataMap,
    timeout: Duration,
}

impl StreamClientBuilder {
    /// Use the given `token` to authenticate with the server.
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Use the given `metadata` when connecting to the server.
    ///
    /// Notice: metadata will be merged with the authentication header if any.
    pub fn with_metadata(mut self, metadata: MetadataMap) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set the maximum time to wait for a message from the server.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum message size that the client can receive.
    pub fn with_max_message_size(mut self, message_size: usize) -> Self {
        self.max_message_size = Some(message_size);
        self
    }

    /// Create and connect to the stream at the given url.
    ///
    /// If a configuration was provided, the client will immediately send it to the server upon
    /// connecting.
    pub async fn connect(self, url: Uri) -> Result<StreamClient, StreamClientError> {
        let channel = Channel::builder(url)
            .connect()
            .await
            .change_context(StreamClientError)
            .attach_printable("failed to connect to the DNA stream")?;
        let mut interceptor = MetadataInterceptor::with_metadata(self.metadata);
        if let Some(token) = self.token {
            interceptor
                .insert_bearer_token(token)
                .change_context(StreamClientError)
                .attach_printable("failed to insert bearer token into metadata")?;
        }

        let mut default_client = DnaStreamClient::with_interceptor(channel, interceptor);
        default_client = if let Some(max_message_size) = self.max_message_size {
            default_client.max_decoding_message_size(max_message_size)
        } else {
            default_client
        };

        Ok(StreamClient::new(default_client, self.timeout))
    }
}

impl Default for StreamClientBuilder {
    fn default() -> Self {
        Self {
            token: None,
            max_message_size: None,
            metadata: MetadataMap::new(),
            timeout: Duration::from_secs(45),
        }
    }
}
