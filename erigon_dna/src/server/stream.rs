//! Implements the node stream service.

use std::{
    path::{Path, PathBuf},
    pin::Pin,
};

use apibara_core::node::v1alpha2::{stream_server, StreamDataRequest, StreamDataResponse};
use apibara_node::{
    server::RequestObserver,
    stream::{new_data_stream, ResponseStream, StreamConfigurationStream},
};
use futures::Stream;
use tonic::{Request, Response, Streaming};
use tracing::info;

use crate::{
    access::Erigon,
    remote::{proto::remote::SnapshotsRequest, KvClient, RemoteDB},
    snapshot::reader::SnapshotReader,
    stream::FilteredDataStream,
};

pub struct StreamService<O: RequestObserver> {
    private_api_url: String,
    datadir: PathBuf,
    request_observer: O,
}

impl<O> StreamService<O>
where
    O: RequestObserver,
{
    pub fn new(private_api_url: String, datadir: &Path, request_observer: O) -> Self {
        StreamService {
            private_api_url,
            datadir: datadir.to_path_buf(),
            request_observer,
        }
    }

    pub fn into_service(self) -> stream_server::StreamServer<Self> {
        stream_server::StreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<O> stream_server::Stream for StreamService<O>
where
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
        // let stream_span = self.request_observer.stream_data_span(request.metadata());
        // let stream_meter = self.request_observer.stream_data_meter(request.metadata());

        /*
        let mut client = KvClient::connect(self.private_api_url.clone())
            .await
            .expect("failed to connect to private api");
        let version = client
            .version(())
            .await
            .expect("failed to fetch private api version")
            .into_inner();
        info!(version = ?version, "connected to KV");

        let snapshots = client
            .snapshots(SnapshotsRequest::default())
            .await
            .expect("failed to fetch snapshots list")
            .into_inner();
        info!(snapshots = ?snapshots, "snapshots");
        let remote_db = RemoteDB::new(client);

        let snapshots_dir = Path::new(&self.datadir).join("snapshots");
        let mut snapshot_reader = SnapshotReader::new(snapshots_dir);
        snapshot_reader
            .reopen_snapshots(snapshots.blocks_files.iter())
            .expect("failed to open snapshots");

        let erigon = Erigon::new(snapshot_reader, remote_db);

        let configuration_stream = StreamConfigurationStream::new(request.into_inner());
        let ingestion_stream = futures::stream::pending();

        /*
        let ingestion_stream = self.ingestion.subscribe().await;
        let ingestion_stream = IngestionStream::new(ingestion_stream);
        */
        let data_stream = FilteredDataStream::new(erigon);

        let data_stream = DataStream::new(configuration_stream, ingestion_stream, data_stream);

        let response = ResponseStream::new(data_stream); // .instrument(stream_span);
        Ok(Response::new(Box::pin(response)))
        */
        todo!()
    }

    async fn stream_data_immutable(
        &self,
        request: Request<StreamDataRequest>,
    ) -> Result<Response<Self::StreamDataImmutableStream>, tonic::Status> {
        todo!()
    }
}

/*
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
*/
