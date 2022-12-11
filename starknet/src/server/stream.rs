//! Implements the node stream service.

use pin_project::pin_project;
use std::{pin::Pin, task::Poll};

use futures::Stream;
use tonic::{Request, Response, Streaming};
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

use crate::core::pb;

pub struct StreamService {}

type ClientStream = Streaming<pb::stream::v1alpha2::StreamDataRequest>;

#[pin_project]
pub struct StreamDataStream {
    #[pin]
    client_stream: ClientStream,
    state: StreamDataState,
}

enum StreamDataState {
    NotConfigured,
    FinalizedStream,
    AcceptedStream,
}

impl StreamService {
    pub fn new() -> Self {
        StreamService {}
    }

    pub fn into_service(self) -> pb::stream::v1alpha2::stream_server::StreamServer<Self> {
        pb::stream::v1alpha2::stream_server::StreamServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::stream::v1alpha2::stream_server::Stream for StreamService {
    type StreamDataStream = Pin<
        Box<
            dyn Stream<Item = Result<pb::stream::v1alpha2::StreamDataResponse, tonic::Status>>
                + Send
                + 'static,
        >,
    >;

    async fn stream_data(
        &self,
        request: Request<Streaming<pb::stream::v1alpha2::StreamDataRequest>>,
    ) -> Result<Response<Self::StreamDataStream>, tonic::Status> {
        let client_stream = request.into_inner();
        let stream = StreamDataStream::new(client_stream).instrument(debug_span!("stream_data"));
        Ok(Response::new(Box::pin(stream)))
    }
}

impl StreamDataStream {
    pub fn new(client_stream: ClientStream) -> Self {
        StreamDataStream {
            client_stream,
            state: StreamDataState::NotConfigured,
        }
    }
}

impl Stream for StreamDataStream {
    type Item = Result<pb::stream::v1alpha2::StreamDataResponse, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match Pin::new(&mut this.client_stream).poll_next(cx) {
            Poll::Pending => {}
            Poll::Ready(None) => {
                debug!("client closed stream");
                // return Poll::Ready(None)
            }
            Poll::Ready(Some(Err(status))) => {
                debug!(status = ?status, "client error");
                return Poll::Ready(None);
            }
            Poll::Ready(Some(Ok(request))) => {
                debug!(request = ?request, "client request");
                // TODO: reconfigured stream
            }
        }

        match this.state {
            StreamDataState::NotConfigured => Poll::Pending,
            StreamDataState::FinalizedStream => Poll::Pending,
            StreamDataState::AcceptedStream => Poll::Pending,
        }
    }
}
