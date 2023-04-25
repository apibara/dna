//! Implements the node stream service.

use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use apibara_core::node::v1alpha2::{stream_server, StreamDataRequest, StreamDataResponse};
use apibara_node::heartbeat::Heartbeat;
use futures::Stream;
use pin_project::pin_project;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Streaming};
use tracing::warn;
use tracing_futures::Instrument;

use crate::{
    core::IngestionMessage,
    db::StorageReader,
    healer::HealerClient,
    // stream::{BatchDataStream, BatchMessage, StreamError},
    ingestion::IngestionStreamClient,
    stream::{DataStream, StreamConfigurationStream, StreamError},
};

use super::metadata::RequestObserver;

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
        
        let data_stream = DataStream::new(
            configuration_stream,
            ingestion_stream,
            self.storage.clone(),
            self.healer.clone(),
            Arc::new(stream_meter),
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
        let (inner_tx, inner_rx) = mpsc::channel::<Result<StreamDataRequest, tonic::Status>>(128);
        let configuration_stream = ReceiverStream::new(inner_rx);
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
        // inject configuration to stream
        inner_tx.send(Ok(stream_data_request)).await;

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
