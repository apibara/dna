//! Implements the node stream service.

use apibara_node::db::libmdbx::{Environment, EnvironmentKind};
use pin_project::pin_project;
use std::{pin::Pin, sync::Arc, task::Poll};

use futures::Stream;
use tonic::{Request, Response, Streaming};
use tracing::{debug, debug_span};
use tracing_futures::Instrument;

use crate::{
    core::pb,
    ingestion::{IngestionStream, IngestionStreamClient},
    stream::{BlockDataAggregator, DatabaseBlockDataAggregator, FinalizedBlockStream},
};

pub struct StreamService<E: EnvironmentKind> {
    ingestion: Arc<IngestionStreamClient>,
    db: Arc<Environment<E>>,
}

type ClientStream = Streaming<pb::stream::v1alpha2::StreamDataRequest>;

#[pin_project]
pub struct StreamDataStream<E: EnvironmentKind> {
    #[pin]
    client_stream: ClientStream,
    #[pin]
    ingestion_stream: IngestionStream,
    state: StreamDataState<E>,
}

struct StreamDataState<E: EnvironmentKind> {
    _phantom: std::marker::PhantomData<E>,
}

impl<E: EnvironmentKind> StreamService<E> {
    pub fn new(ingestion: Arc<IngestionStreamClient>, db: Arc<Environment<E>>) -> Self {
        StreamService { ingestion, db }
    }

    pub fn into_service(self) -> pb::stream::v1alpha2::stream_server::StreamServer<Self> {
        pb::stream::v1alpha2::stream_server::StreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<E: EnvironmentKind> pb::stream::v1alpha2::stream_server::Stream for StreamService<E> {
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
        /*
        let client_stream = request.into_inner();
        let ingestion_stream = self.ingestion.subscribe().await;
        let stream = StreamDataStream::new(client_stream, ingestion_stream, self.db.clone())
            .instrument(debug_span!("stream_data"));
        Ok(Response::new(Box::pin(stream)))
        */
        todo!()
    }
}

impl<E: EnvironmentKind> StreamDataStream<E> {
    pub fn new(
        client_stream: ClientStream,
        ingestion_stream: IngestionStream,
        db: Arc<Environment<E>>,
    ) -> Self {
        /*
        let state = StreamDataState::NotConfigured { db };
        StreamDataStream {
            client_stream,
            ingestion_stream,
            state,
        }
        */
        todo!()
    }
}

impl<E: EnvironmentKind> Stream for StreamDataStream<E> {
    type Item = Result<pb::stream::v1alpha2::StreamDataResponse, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
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
                // this.state.handle_request(request);
                // TODO: reconfigured stream
            }
        }

        match Pin::new(&mut this.ingestion_stream).poll_next(cx) {
            Poll::Pending => {}
            Poll::Ready(None) => {}
            Poll::Ready(message) => {
                debug!(message = ?message, "got message");
            }
        }

        todo!()
    }
}

/*
impl<E: EnvironmentKind> StreamDataState<E> {
    fn handle_request(&mut self, request: pb::stream::v1alpha2::StreamDataRequest) {
        match self {
            Self::NotConfigured { db } => {
                let filter = request.filter.unwrap();
                let aggregator = DatabaseBlockDataAggregator::new(db.clone(), filter);
            }
            _ => todo!()
        }
        // TODO: create correct stream
        todo!()
    }
}
*/
