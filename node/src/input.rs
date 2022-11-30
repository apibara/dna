use std::{pin::Pin, task};

use apibara_core::{
    application::pb as app_pb,
    node::v1alpha1::pb::{self as node_pb, node_client::NodeClient},
};
use futures::{Stream, StreamExt};
use tokio_stream::StreamMap;
use tonic::Streaming;

/// Turn multiple input streams into a single one.
///
/// Note: at the moment, [InputMixer] will panic if
/// actually using multiple input streams.
pub struct InputMixer {
    inputs: Vec<app_pb::InputStream>,
}

#[derive(Debug, thiserror::Error)]
pub enum InputMixerError {
    #[error("tonic connection error")]
    Connection(#[from] tonic::transport::Error),
    #[error("streaming error")]
    Stream(#[from] tonic::Status),
}

impl InputMixer {
    /// Creates a new [InputMixer] from the protobuf-encoded configuration.
    pub fn new_from_pb(inputs: Vec<app_pb::InputStream>) -> Self {
        if inputs.len() != 1 {
            panic!("unsupported multiple inputs");
        }
        Self { inputs }
    }

    /// Start streaming messages from the configured inputs.
    pub async fn stream_messages(self) -> Result<MixedInputStream, InputMixerError> {
        let mut inner = StreamMap::with_capacity(self.inputs.len());
        for input_config in self.inputs {
            let url = format!("https://{}", input_config.url);
            let mut client = NodeClient::connect(url).await?;
            let stream_request = node_pb::StreamMessagesRequest {
                starting_sequence: input_config.starting_sequence,
                pending_block_interval_seconds: 0,
            };
            let client_stream = client.stream_messages(stream_request).await?.into_inner();
            inner.insert(input_config.id, client_stream);
        }
        Ok(MixedInputStream { inner })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MixedInputStreamError {
    #[error("received a response error")]
    Response { input_id: u64, error: tonic::Status },
}

pub struct MixedInputStream {
    inner: StreamMap<u64, Streaming<node_pb::StreamMessagesResponse>>,
}

impl Stream for MixedInputStream {
    type Item = Result<(u64, node_pb::StreamMessagesResponse), MixedInputStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map(|maybe_message| {
            maybe_message.map(|(input_id, message)| {
                message
                    .map(|m| (input_id, m))
                    .map_err(|error| MixedInputStreamError::Response { input_id, error })
            })
        })
    }
}
