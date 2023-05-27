//! Implements the node stream service.

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

use apibara_core::node::v1alpha2::{stream_server, StreamDataRequest, StreamDataResponse};
use apibara_node::{
    server::RequestObserver,
    stream::{new_data_stream, ResponseStream, StreamConfigurationStream, StreamError},
};
use futures::Stream;
use pin_project::pin_project;
use tonic::{metadata::MetadataMap, Request, Response, Streaming};
use tracing_futures::Instrument;

use crate::{
    core::IngestionMessage,
    db::StorageReader,
    ingestion::IngestionStreamClient,
    stream::{DbBatchProducer, SequentialCursorProducer},
};

pub struct StreamService<R: StorageReader, O: RequestObserver> {
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
    request_observer: O,
}

impl<R, O> StreamService<R, O>
where
    R: StorageReader + Send + Sync + 'static,
    O: RequestObserver,
{
    pub fn new(ingestion: Arc<IngestionStreamClient>, storage: R, request_observer: O) -> Self {
        let storage = Arc::new(storage);
        StreamService {
            ingestion,
            storage,
            request_observer,
        }
    }

    pub fn into_service(self) -> stream_server::StreamServer<Self> {
        stream_server::StreamServer::new(self)
    }

    async fn stream_data_with_configuration<S, E>(
        &self,
        metadata: MetadataMap,
        configuration: S,
    ) -> impl Stream<Item = Result<StreamDataResponse, tonic::Status>>
    where
        S: Stream<Item = Result<StreamDataRequest, E>> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        let stream_span = self.request_observer.stream_data_span(&metadata);
        let stream_meter = self.request_observer.stream_data_meter(&metadata);

        let configuration_stream = StreamConfigurationStream::new(configuration);
        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        let batch_producer = DbBatchProducer::new(self.storage.clone());
        let cursor_producer = SequentialCursorProducer::new(self.storage.clone());

        let data_stream = new_data_stream(
            configuration_stream,
            ingestion_stream,
            cursor_producer,
            batch_producer,
            stream_meter,
        );

        ResponseStream::new(data_stream).instrument(stream_span)
    }
}

#[tonic::async_trait]
impl<R, O> stream_server::Stream for StreamService<R, O>
where
    R: StorageReader + Send + Sync + 'static,
    O: RequestObserver,
{
    type StreamDataStream =
        Pin<Box<dyn Stream<Item = Result<StreamDataResponse, tonic::Status>> + Send + 'static>>;

    type StreamDataImmutableStream =
        Pin<Box<dyn Stream<Item = Result<StreamDataResponse, tonic::Status>> + Send + 'static>>;

    async fn stream_data(
        &self,
        request: Request<Streaming<StreamDataRequest>>,
    ) -> Result<Response<Self::StreamDataStream>, tonic::Status> {
        let metadata = request.metadata().clone();
        let response = self
            .stream_data_with_configuration(metadata, request.into_inner())
            .await;
        Ok(Response::new(Box::pin(response)))
    }

    async fn stream_data_immutable(
        &self,
        request: Request<StreamDataRequest>,
    ) -> Result<Response<Self::StreamDataImmutableStream>, tonic::Status> {
        let metadata = request.metadata().clone();
        let configuration_stream = ImmutableRequestStream {
            request: Some(request.into_inner()),
        };
        let response = self
            .stream_data_with_configuration(metadata, configuration_stream)
            .await;
        Ok(Response::new(Box::pin(response)))
    }
}

/// A stream that yields the configuration once, and is pending forever after that.
struct ImmutableRequestStream {
    request: Option<StreamDataRequest>,
}

impl Stream for ImmutableRequestStream {
    type Item = Result<StreamDataRequest, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.request.take() {
            Some(request) => Poll::Ready(Some(Ok(request))),
            None => Poll::Pending,
        }
    }
}

/// A simple adapter from a generic ingestion stream to the one used by the server/stream module.
#[pin_project]
pub struct IngestionStream<L, E>
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
