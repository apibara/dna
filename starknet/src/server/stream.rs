//! Implements the node stream service.

use pin_project::pin_project;
use std::{pin::Pin, sync::Arc, task::Poll};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tracing_futures::Instrument;

use futures::{Stream, TryFutureExt, TryStreamExt};
use tonic::{Request, Response, Streaming};
use tracing::{info, trace_span};

use crate::{
    core::{pb, GlobalBlockId},
    db::StorageReader,
    ingestion::{IngestionStream, IngestionStreamClient},
    stream::{DatabaseBlockDataAggregator, FinalizedBlockStream},
};

pub struct StreamService<R: StorageReader> {
    ingestion: Arc<IngestionStreamClient>,
    storage: Arc<R>,
}

// type ClientStream = Streaming<pb::stream::v1alpha2::StreamDataRequest>;

#[pin_project]
pub struct StreamDataStream<R: StorageReader> {
    #[pin]
    inner: FinalizedBlockStream<
        DatabaseBlockDataAggregator<R>,
        IngestionStream,
        BroadcastStreamRecvError,
    >,
}

impl<R> StreamService<R>
where
    R: StorageReader + Send + Sync + 'static,
{
    pub fn new(ingestion: Arc<IngestionStreamClient>, storage: R) -> Self {
        let storage = Arc::new(storage);
        StreamService { ingestion, storage }
    }

    pub fn into_service(self) -> pb::stream::v1alpha2::stream_server::StreamServer<Self> {
        pb::stream::v1alpha2::stream_server::StreamServer::new(self)
    }

    async fn create_stream(
        &self,
        starting_block: GlobalBlockId,
        filter: pb::starknet::v1alpha2::Filter,
    ) -> StreamDataStream<R> {
        let ingestion_stream = self.ingestion.subscribe().await;
        StreamDataStream::new(
            ingestion_stream,
            self.storage.clone(),
            starting_block,
            filter,
        )
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
        let mut client_stream = request.into_inner();
        let request = client_stream
            .try_next()
            .await
            .map_err(|_| tonic::Status::internal("failed to read client stream"))?
            .ok_or_else(|| tonic::Status::internal("failed to read client stream"))?;

        // TODO
        // - starting block should come from `request.starting_cursor`
        // - assign `stream_id`
        // - use `finality`
        let starting_block = self
            .storage
            .canonical_block_id(0)
            .expect("database")
            .expect("genesis");

        let filter = request.filter.unwrap_or_default();

        let stream = self
            .create_stream(starting_block, filter)
            .await
            .instrument(trace_span!("stream_data"));
        Ok(Response::new(Box::pin(stream)))
    }
}

impl<R: StorageReader> StreamDataStream<R> {
    pub fn new(
        ingestion_stream: IngestionStream,
        storage: Arc<R>,
        starting_block: GlobalBlockId,
        filter: pb::starknet::v1alpha2::Filter,
    ) -> Self {
        let highest_finalized = storage.highest_finalized_block().unwrap().unwrap();
        let aggregator = DatabaseBlockDataAggregator::new(storage, filter);
        let inner = FinalizedBlockStream::new(
            starting_block,
            highest_finalized,
            aggregator,
            ingestion_stream,
        );
        StreamDataStream { inner }
    }
}

impl<R: StorageReader> Stream for StreamDataStream<R> {
    type Item = Result<pb::stream::v1alpha2::StreamDataResponse, tonic::Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => {
                let err = Err(tonic::Status::internal("stream fail"));
                return Poll::Ready(Some(err));
            }
            Poll::Ready(Some(Ok(data))) => {
                let response = pb::stream::v1alpha2::StreamDataResponse {
                    stream_id: 0,
                    message: Some(pb::stream::v1alpha2::stream_data_response::Message::Data(
                        data,
                    )),
                };
                Poll::Ready(Some(Ok(response)))
            }
        }
    }
}
