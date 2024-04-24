use apibara_dna_protocol::dna::stream::dna_stream_client::DnaStreamClient;
use error_stack::{Result, ResultExt};
use tonic::transport::Channel;
use tracing::error;

use crate::{error::SinkError, SinkErrorReportExt, StreamConfiguration};

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

    pub async fn new_stream_client(&self) -> Result<DnaStreamClient<Channel>, SinkError> {
        /*
        let mut stream_builder = ClientBuilder::default()
            .with_max_message_size(
                self.stream_configuration.max_message_size_bytes.as_u64() as usize
            )
            .with_metadata(self.stream_configuration.metadata.clone())
            .with_timeout(self.stream_configuration.timeout_duration);

        stream_builder = if let Some(bearer_token) = self.stream_configuration.bearer_token.clone()
        {
            stream_builder.with_bearer_token(Some(bearer_token))
        } else {
            stream_builder
        };
        */
        error!("StreamClientFactory::new_stream_client not implemented");

        let client = DnaStreamClient::connect(self.stream_configuration.stream_url.clone())
            .await
            .change_context(SinkError::Temporary)
            .attach_printable("failed to connect to stream")?;

        Ok(client)
    }
}
