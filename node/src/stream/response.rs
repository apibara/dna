use std::{
    pin::Pin,
    task::{self, Poll},
    time::Duration,
};

use apibara_core::node::v1alpha2::StreamDataResponse;
use futures::Stream;
use pin_project::pin_project;

use super::{error::StreamError, heartbeat::Heartbeat};

#[pin_project]
pub struct ResponseStream<S>
where
    S: Stream<Item = Result<StreamDataResponse, StreamError>>,
{
    #[pin]
    inner: Heartbeat<S>,
}

impl<S> ResponseStream<S>
where
    S: Stream<Item = Result<StreamDataResponse, StreamError>>,
{
    pub fn new(inner: S) -> Self {
        let inner = Heartbeat::new(inner, Duration::from_secs(30));
        ResponseStream { inner }
    }
}

impl<S> Stream for ResponseStream<S>
where
    S: Stream<Item = Result<StreamDataResponse, StreamError>> + Unpin,
{
    type Item = Result<StreamDataResponse, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(value)) => {
                let response = match value {
                    Err(_) => {
                        // heartbeat
                        use apibara_core::node::v1alpha2::{
                            stream_data_response::Message, Heartbeat,
                        };

                        // stream_id is not relevant for heartbeat messages
                        let response = StreamDataResponse {
                            stream_id: 0,
                            message: Some(Message::Heartbeat(Heartbeat {})),
                        };
                        Ok(response)
                    }
                    Ok(Err(err)) => Err(err.into_status()),
                    Ok(Ok(response)) => Ok(response),
                };
                Poll::Ready(Some(response))
            }
        }
    }
}
