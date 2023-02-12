//! Implements the node stream service.

use apibara_node::heartbeat::Heartbeat;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use futures::Stream;
use tonic::{Request, Response, Streaming};
use tracing::warn;
use tracing_futures::Instrument;

use crate::{
    core::{
        pb::{self, stream::v1alpha2::StreamDataResponse},
        IngestionMessage,
    },
    db::StorageReader,
    healer::HealerClient,
    // stream::{BatchDataStream, BatchMessage, StreamError},
    ingestion::IngestionStreamClient,
    stream::{DataStream, StreamConfigurationStream, StreamError},
};

use super::span::RequestSpan;

pub struct StreamService<R: StorageReader> {
    ingestion: Arc<IngestionStreamClient>,
    healer: Arc<HealerClient>,
    storage: Arc<R>,
    request_span: Arc<dyn RequestSpan>,
}

impl<R> StreamService<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    pub fn new(
        ingestion: Arc<IngestionStreamClient>,
        healer: Arc<HealerClient>,
        storage: R,
        request_span: Arc<dyn RequestSpan>,
    ) -> Self {
        let storage = Arc::new(storage);
        StreamService {
            ingestion,
            healer,
            storage,
            request_span,
        }
    }

    pub fn into_service(self) -> pb::stream::v1alpha2::stream_server::StreamServer<Self> {
        pb::stream::v1alpha2::stream_server::StreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<R> pb::stream::v1alpha2::stream_server::Stream for StreamService<R>
where
    R: StorageReader + Send + Sync + 'static,
{
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
        let stream_span = self.request_span.stream_data_span(request.metadata());

        let configuration_stream = StreamConfigurationStream::new(request.into_inner());

        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);

        let data_stream = DataStream::new(
            configuration_stream,
            ingestion_stream,
            self.storage.clone(),
            self.healer.clone(),
        );

        let response = ResponseStream::new(data_stream).instrument(stream_span);
        Ok(Response::new(Box::pin(response)))
    }
}

/// A simple adapter from a generic ingestion stream to the one used by the server/stream module.
#[pin_project]
struct IngestionStream<L, E>
where
    L: Stream<Item = Result<IngestionMessage, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    #[pin]
    inner: L,
}
impl<L, E> IngestionStream<L, E>
where
    L: Stream<Item = Result<IngestionMessage, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(inner: L) -> Self {
        IngestionStream { inner }
    }
}

impl<L, E> Stream for IngestionStream<L, E>
where
    L: Stream<Item = Result<IngestionMessage, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<IngestionMessage, StreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(Ok(value))),
            Poll::Ready(Some(Err(err))) => {
                let err = StreamError::internal(err);
                Poll::Ready(Some(Err(err)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[pin_project]
struct ResponseStream<S>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::StreamDataResponse, StreamError>>,
{
    #[pin]
    inner: Heartbeat<S>,
}

impl<S> ResponseStream<S>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::StreamDataResponse, StreamError>>,
{
    pub fn new(inner: S) -> Self {
        let inner = Heartbeat::new(inner, Duration::from_secs(30));
        ResponseStream { inner }
    }
}

impl<S> Stream for ResponseStream<S>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::StreamDataResponse, StreamError>> + Unpin,
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
                        use pb::stream::v1alpha2::{stream_data_response::Message, Heartbeat};

                        // stream_id is not relevant for heartbeat messages
                        let response = StreamDataResponse {
                            stream_id: 0,
                            message: Some(Message::Heartbeat(Heartbeat {})),
                        };
                        Ok(response)
                    }
                    Ok(Err(err)) => {
                        let status = match err {
                            StreamError::Client { message } => {
                                tonic::Status::invalid_argument(message)
                            }
                            StreamError::Internal(err) => {
                                warn!(err = ?err, "stream service error");
                                tonic::Status::internal("internal server error")
                            }
                        };
                        Err(status)
                    }
                    Ok(Ok(response)) => Ok(response),
                };
                Poll::Ready(Some(response))
            }
        }
    }
}
