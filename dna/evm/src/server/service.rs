use apibara_dna_common::storage::StorageBackend;
use apibara_dna_protocol::dna::{
    dna_stream_server, StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

type TonicResult<T> = std::result::Result<T, tonic::Status>;

pub struct Service<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    storage: S,
}

impl<S> Service<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(storage: S) -> Self {
        Self { storage }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<Service<S>> {
        dna_stream_server::DnaStreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<S> dna_stream_server::DnaStream for Service<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    type StreamDataStream = ReceiverStream<TonicResult<StreamDataResponse>>;

    async fn stream_data(
        &self,
        request: Request<StreamDataRequest>,
    ) -> TonicResult<tonic::Response<Self::StreamDataStream>> {
        todo!()
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> TonicResult<tonic::Response<StatusResponse>> {
        todo!()
    }
}
