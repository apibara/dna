use std::{fmt, pin::Pin, task::Poll, time::Duration};

use pin_project::pin_project;
use tokio_stream::{Stream, StreamExt, Timeout};
use tonic::{service::interceptor::InterceptedService, transport::Channel, IntoRequest, Streaming};

use crate::dna::{
    common::{StatusRequest, StatusResponse},
    stream::{
        dna_stream_client::DnaStreamClient, stream_data_response, StreamDataRequest,
        StreamDataResponse,
    },
};

use super::MetadataInterceptor;

pub type StreamMessage = stream_data_response::Message;

#[derive(Debug)]
pub enum DataStreamError {
    Timeout,
    Tonic(tonic::Status),
}

#[derive(Debug)]
#[pin_project]
pub struct DataStream {
    #[pin]
    inner: Pin<Box<Timeout<Streaming<StreamDataResponse>>>>,
}

/// Data stream client.
#[derive(Clone)]
pub struct StreamClient {
    inner: DnaStreamClient<InterceptedService<Channel, MetadataInterceptor>>,
    timeout: Duration,
}

impl StreamClient {
    pub(crate) fn new(
        inner: DnaStreamClient<InterceptedService<Channel, MetadataInterceptor>>,
        timeout: Duration,
    ) -> Self {
        Self { inner, timeout }
    }

    pub async fn stream_data(
        &mut self,
        request: impl IntoRequest<StreamDataRequest>,
    ) -> Result<DataStream, tonic::Status> {
        let response = self.inner.stream_data(request).await?;
        let inner = response.into_inner().timeout(self.timeout.clone());
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
                Err(_elapsed) => Poll::Ready(Some(Err(DataStreamError::Timeout))),
                Ok(Err(tonic_error)) => Poll::Ready(Some(Err(DataStreamError::Tonic(tonic_error)))),
                Ok(Ok(response)) => {
                    if let Some(message) = response.message {
                        Poll::Ready(Some(Ok(message)))
                    } else {
                        let error = tonic::Status::data_loss("missing message in response");
                        Poll::Ready(Some(Err(DataStreamError::Tonic(error))))
                    }
                }
            },
        }
    }
}

impl error_stack::Context for DataStreamError {}

impl fmt::Display for DataStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataStreamError::Timeout => write!(f, "data stream timeout"),
            DataStreamError::Tonic(status) => write!(
                f,
                "data stream error: {} - {}",
                status.code(),
                status.message()
            ),
        }
    }
}
