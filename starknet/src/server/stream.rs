//! Implements the node stream service.

use apibara_node::heartbeat::Heartbeat;
use pin_project::pin_project;
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};
use tracing_futures::Instrument;

use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Streaming};
use tracing::warn;

use crate::{
    core::{
        pb::{self, stream::v1alpha2::StreamDataResponse},
        GlobalBlockId,
    },
    db::StorageReader,
    ingestion::IngestionStreamClient,
    stream::{BatchDataStream, BatchDataStreamExt, BatchMessage, FinalizedBlockStream},
};

use super::span::RequestSpan;

pub struct StreamService<R: StorageReader> {
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
    request_span: Arc<dyn RequestSpan>,
}

// type ClientStream = Streaming<pb::stream::v1alpha2::StreamDataRequest>;

impl<R> StreamService<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    pub fn new(
        ingestion: Arc<IngestionStreamClient>,
        storage: R,
        request_span: Arc<dyn RequestSpan>,
    ) -> Self {
        let storage = Arc::new(storage);
        StreamService {
            ingestion,
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
        use pb::stream::v1alpha2::DataFinality;

        let stream_span = self.request_span.stream_data_span(request.metadata());

        let mut client_stream = request.into_inner();
        let initial_request = client_stream
            .try_next()
            .await
            .map_err(internal_error)?
            .ok_or_else(mk_internal_error)?;

        let filter = initial_request.filter.unwrap_or_default();
        let starting_cursor = match initial_request.starting_cursor {
            None => {
                // start from genesis
                self.storage
                    .canonical_block_id(0)
                    .map_err(internal_error)?
                    .ok_or_else(mk_internal_error)?
            }
            Some(cursor) => GlobalBlockId::from_cursor(&cursor).map_err(internal_error)?,
        };
        let requested_finality = initial_request.finality.and_then(DataFinality::from_i32);
        match requested_finality {
            Some(DataFinality::DataStatusPending) => {
                return Err(tonic::Status::internal("pending data not yet implemented"));
            }
            Some(DataFinality::DataStatusFinalized) => {
                let ingestion_stream = self.ingestion.subscribe().await;
                let finalized_cursor = self
                    .storage
                    .highest_finalized_block()
                    .map_err(internal_error)?
                    .ok_or_else(mk_internal_error)?;
                let inner_stream = FinalizedBlockStream::new(
                    starting_cursor,
                    finalized_cursor,
                    filter,
                    self.storage.clone(),
                    ingestion_stream,
                )
                .map_err(internal_error)?;

                let response = inner_stream
                    .batch(50, Duration::from_millis(250))
                    .stream_data_response()
                    .instrument(stream_span);

                Ok(Response::new(Box::pin(response)))
            }
            _ => {
                // default to accepted
                todo!()
            }
        }
    }
}

trait StreamDataStreamExt: Stream {
    type Error: std::error::Error;

    fn stream_data_response(self) -> StreamDataStream<Self, Self::Error>
    where
        Self: Stream<Item = Result<pb::stream::v1alpha2::Data, Self::Error>> + Sized;
}

impl<S, E> StreamDataStreamExt for BatchDataStream<S, E>
where
    S: Stream<Item = Result<BatchMessage, E>>,
    E: std::error::Error,
{
    type Error = E;

    fn stream_data_response(self) -> StreamDataStream<Self, Self::Error>
    where
        Self: Stream<Item = Result<pb::stream::v1alpha2::Data, Self::Error>> + Sized,
    {
        StreamDataStream::new(self)
    }
}

#[pin_project]
struct StreamDataStream<S, E>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::Data, E>>,
    E: std::error::Error,
{
    #[pin]
    inner: Heartbeat<S>,
}

impl<S, E> StreamDataStream<S, E>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::Data, E>>,
    E: std::error::Error,
{
    pub fn new(inner: S) -> Self {
        let inner = Heartbeat::new(inner, Duration::from_secs(30));
        StreamDataStream { inner }
    }
}

impl<S, E> Stream for StreamDataStream<S, E>
where
    S: Stream<Item = Result<pb::stream::v1alpha2::Data, E>> + Unpin,
    E: std::error::Error,
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

                        // TODO: stream id should come from inner stream
                        let response = StreamDataResponse {
                            stream_id: 0,
                            message: Some(Message::Heartbeat(Heartbeat {})),
                        };
                        Ok(response)
                    }
                    Ok(Err(err)) => {
                        // inner error
                        Err(internal_error(err))
                    }
                    Ok(Ok(data)) => {
                        // data
                        use pb::stream::v1alpha2::stream_data_response::Message;
                        // TODO: invalidate messages should come from inner stream

                        let response = StreamDataResponse {
                            stream_id: 0,
                            message: Some(Message::Data(data)),
                        };
                        Ok(response)
                    }
                };
                Poll::Ready(Some(response))
            }
        }
    }
}

fn mk_internal_error() -> tonic::Status {
    tonic::Status::internal("internal server error")
}

fn internal_error<E: std::error::Error>(err: E) -> tonic::Status {
    warn!(err = ?err, "stream service error");
    mk_internal_error()
}
