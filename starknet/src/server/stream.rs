//! Implements the node stream service.

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use apibara_core::node::v1alpha2::{stream_server, StreamDataRequest, StreamDataResponse};
use apibara_node::{
    server::RequestObserver,
    stream::{new_data_stream, ResponseStream, StreamConfigurationStream, StreamError},
};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use tonic::{Request, Response, Streaming};
use tracing::warn;
use tracing_futures::Instrument;

use crate::{
    core::{GlobalBlockId, IngestionMessage},
    db::StorageReader,
    healer::HealerClient,
    ingestion::IngestionStreamClient,
    stream::{DbBatchProducer, SequentialCursorProducer},
};

pub struct StreamService<R: StorageReader, O: RequestObserver> {
    ingestion: Arc<IngestionStreamClient>,
    healer: Arc<HealerClient>,
    storage: Arc<R>,
    request_observer: O,
}

impl<R, O> StreamService<R, O>
where
    R: StorageReader + Send + Sync + 'static,
    O: RequestObserver,
{
    pub fn new(
        ingestion: Arc<IngestionStreamClient>,
        healer: Arc<HealerClient>,
        storage: R,
        request_observer: O,
    ) -> Self {
        let storage = Arc::new(storage);
        StreamService {
            ingestion,
            healer,
            storage,
            request_observer,
        }
    }

    pub fn into_service(self) -> stream_server::StreamServer<Self> {
        stream_server::StreamServer::new(self)
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
        let stream_span = self.request_observer.stream_data_span(request.metadata());
        let stream_meter = self.request_observer.stream_data_meter(request.metadata());

        let configuration_stream = StreamConfigurationStream::new(request.into_inner());
        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        let batch_producer = DbBatchProducer::new(self.storage.clone());
        let cursor_producer = SequentialCursorProducer::new(self.storage.clone());
        let data_stream = new_data_stream(
            configuration_stream,
            ingestion_stream,
            cursor_producer,
            batch_producer,
        );

        let response = ResponseStream::new(data_stream).instrument(stream_span);
        Ok(Response::new(Box::pin(response)))
    }

    async fn stream_data_immutable(
        &self,
        request: Request<StreamDataRequest>,
    ) -> Result<Response<Self::StreamDataImmutableStream>, tonic::Status> {
        let stream_span = self.request_observer.stream_data_span(request.metadata());
        let stream_meter = self.request_observer.stream_data_meter(request.metadata());

        let stream_data_request = request.into_inner();

        let configuration_stream = ImmutableRequestStream {
            request: Some(stream_data_request),
        };

        let configuration_stream = StreamConfigurationStream::new(configuration_stream);
        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        let batch_producer = DbBatchProducer::new(self.storage.clone());
        let cursor_producer = SequentialCursorProducer::new(self.storage.clone());
        let data_stream = new_data_stream(
            configuration_stream,
            ingestion_stream,
            cursor_producer,
            batch_producer,
        );

        let response = ResponseStream::new(data_stream).instrument(stream_span);
        /*
        let configuration_stream = StreamConfigurationStream::new(configuration_stream);

        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);

        let data_stream = DataStream::new(
            configuration_stream,
            ingestion_stream,
            self.storage.clone(),
            self.healer.clone(),
            Arc::new(stream_meter),
        );

        let response = ResponseStream::new(data_stream).instrument(stream_span);
        Ok(Response::new(Box::pin(response)))
        */
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
