use apibara_dna_protocol::client::{StreamClient, StreamClientBuilder};
use error_stack::{Result, ResultExt};

use crate::{error::SinkError, StreamConfiguration};

/// Action to take after handling a batch of data.
#[derive(Debug, PartialEq)]
pub enum StreamAction {
    /// Continue streaming.
    Continue,
    /// Stop streaming.
    Stop,
    /// Reconnect to the stream using the new filter.
    Reconnect,
}

pub struct StreamClientFactory {
    stream_configuration: StreamConfiguration,
}

impl StreamClientFactory {
    pub fn new(stream_configuration: StreamConfiguration) -> Self {
        Self {
            stream_configuration,
        }
    }

    pub async fn new_stream_client(&self) -> Result<StreamClient, SinkError> {
        let mut stream_builder = StreamClientBuilder::default()
            .with_max_message_size(
                self.stream_configuration.max_message_size_bytes.as_u64() as usize
            )
            .with_metadata(self.stream_configuration.metadata.clone())
            .with_timeout(self.stream_configuration.timeout_duration);

        stream_builder = if let Some(bearer_token) = &self.stream_configuration.bearer_token {
            stream_builder.with_bearer_token(bearer_token)
        } else {
            stream_builder
        };

        let client = stream_builder
            .connect(self.stream_configuration.stream_url.clone())
            .await
            .change_context(SinkError::Temporary)
            .attach_printable("failed to connect to stream")?;

        Ok(client)
    }
}
