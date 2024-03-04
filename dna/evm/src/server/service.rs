use apibara_dna_common::error::Result;
use apibara_dna_common::storage::StorageBackend;
use apibara_dna_protocol::{
    dna::{
        dna_stream_server, StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
    },
    evm,
};
use futures_util::{Future, TryFutureExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::{info, warn};

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
    S: StorageBackend + Send + Sync + 'static + Clone,
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
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    type StreamDataStream = ReceiverStream<TonicResult<StreamDataResponse>>;

    async fn stream_data(
        &self,
        request: Request<StreamDataRequest>,
    ) -> TonicResult<tonic::Response<Self::StreamDataStream>> {
        let (tx, rx) = mpsc::channel(128);
        let request = request.into_inner();

        let starting_block_number = request
            .starting_cursor
            .map(|c| c.order_key + 1)
            .unwrap_or_default();

        let filters = request
            .filter
            .into_iter()
            .map(|f| <evm::Filter as prost::Message>::decode(f.as_slice()))
            .collect::<std::result::Result<Vec<evm::Filter>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        tokio::spawn(
            do_stream_data(self.storage.clone(), starting_block_number, filters, tx).inspect_err(
                |err| {
                    warn!(?err, "stream data error");
                },
            ),
        );

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> TonicResult<tonic::Response<StatusResponse>> {
        todo!()
    }
}

fn assert_send<'u, R>(
    fut: impl 'u + Send + Future<Output = R>,
) -> impl 'u + Send + Future<Output = R> {
    fut
}

async fn do_stream_data<S>(
    storage: S,
    starting_block_number: u64,
    filters: Vec<evm::Filter>,
    tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
) -> Result<()>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    info!(starting_block_number, num_filters = %filters.len(), "starting stream");
    Ok(())
}
