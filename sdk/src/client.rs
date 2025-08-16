use std::{pin::Pin, task::Poll, time::Duration};

use apibara_dna_protocol::dna::stream::{
    dna_stream_client::DnaStreamClient, stream_data_response, StatusRequest, StatusResponse,
    StreamDataRequest, StreamDataResponse,
};
use pin_project::pin_project;
use snafu::Snafu;
use tokio_stream::{Elapsed, Stream, StreamExt, Timeout};
use tonic::{service::interceptor::InterceptedService, transport::Channel, Streaming};

use crate::{interceptor::MetadataInterceptor, StreamClientBuilder};

type StreamMessage = stream_data_response::Message;

/// Error type for the data stream client.
#[derive(Debug, Snafu)]
pub enum DataStreamError {
    /// Error caused by the gRPC client.
    #[snafu(display("gRPC client error"))]
    Tonic { source: tonic::Status },
    /// Timeout error. The client did not receive a message in the specified time.
    #[snafu(display("gRPC stream timeout: {elapsed}"))]
    Timeout { elapsed: Elapsed },
    /// The response does not contain any message.
    #[snafu(display("Received empty message in response"))]
    EmptyMessageInResponse,
}

/// A DNA stream returned by [StreamClient::stream_data].
#[derive(Debug)]
#[pin_project]
pub struct DataStream {
    #[pin]
    inner: Pin<Box<Timeout<Streaming<StreamDataResponse>>>>,
}

/// A DNA stream client.
#[derive(Clone)]
pub struct StreamClient {
    inner: DnaStreamClient<InterceptedService<Channel, MetadataInterceptor>>,
    timeout: Duration,
}

impl StreamClient {
    /// Create a new [StreamClientBuilder] to configure the client.
    pub fn builder() -> StreamClientBuilder {
        StreamClientBuilder::new()
    }

    pub(crate) fn new(
        inner: DnaStreamClient<InterceptedService<Channel, MetadataInterceptor>>,
        timeout: Duration,
    ) -> Self {
        Self { inner, timeout }
    }

    /// Start streaming data from the server.
    ///
    /// If not already set, the heartbeat interval is set to half of the timeout
    /// duration.
    pub async fn stream_data(
        &mut self,
        request: impl Into<StreamDataRequest>,
    ) -> Result<DataStream, tonic::Status> {
        let mut request: StreamDataRequest = request.into();
        if request.heartbeat_interval.is_none() {
            let heartbeat = prost_types::Duration {
                seconds: (self.timeout.as_secs() / 2) as _,
                nanos: 0,
            };
            request.heartbeat_interval = Some(heartbeat);
        }

        let response = self.inner.stream_data(request).await?;
        let inner = response.into_inner().timeout(self.timeout);
        Ok(DataStream {
            inner: Box::pin(inner),
        })
    }

    /// Get DNA server status.
    pub async fn status(&mut self) -> Result<StatusResponse, tonic::Status> {
        let request = StatusRequest::default();
        let response = self.inner.status(request).await?;
        Ok(response.into_inner())
    }
}

impl Stream for DataStream {
    type Item = Result<StreamMessage, DataStreamError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(response_or_timeout)) => match response_or_timeout {
                Err(elapsed) => Poll::Ready(Some(Err(DataStreamError::Timeout { elapsed }))),
                Ok(Err(tonic_error)) => Poll::Ready(Some(Err(DataStreamError::Tonic {
                    source: tonic_error,
                }))),
                Ok(Ok(response)) => {
                    if let Some(message) = response.message {
                        Poll::Ready(Some(Ok(message)))
                    } else {
                        Poll::Ready(Some(Err(DataStreamError::EmptyMessageInResponse)))
                    }
                }
            },
        }
    }
}
