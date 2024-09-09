use std::pin::Pin;

use apibara_dna_protocol::dna::stream::{
    dna_stream_server::{self, DnaStream},
    StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
};
use error_stack::Result;
use futures::{Future, Stream};
use tracing::{error, info};

use crate::chain_view::{ChainView, ChainViewError};

pub struct StreamService {
    chain_view: tokio::sync::watch::Receiver<Option<ChainView>>,
}

impl StreamService {
    pub fn new(chain_view: tokio::sync::watch::Receiver<Option<ChainView>>) -> Self {
        Self { chain_view }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<Self> {
        dna_stream_server::DnaStreamServer::new(self)
    }
}

trait ChainViewExt {
    fn get_status(&self) -> impl Future<Output = Result<StatusResponse, ChainViewError>> + Send;
}

#[tonic::async_trait]
impl DnaStream for StreamService {
    type StreamDataStream = Pin<
        Box<dyn Stream<Item = tonic::Result<StreamDataResponse, tonic::Status>> + Send + 'static>,
    >;

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> tonic::Result<tonic::Response<StatusResponse>, tonic::Status> {
        let Some(chain_view) = self.chain_view.borrow().clone() else {
            return Err(tonic::Status::unavailable("chain view not initialized yet"));
        };

        let response = chain_view.get_status().await.map_err(|err| {
            error!(error = ?err, "DnaStream::status error");
            tonic::Status::internal("internal server error")
        })?;

        Ok(tonic::Response::new(response))
    }

    async fn stream_data(
        &self,
        request: tonic::Request<StreamDataRequest>,
    ) -> tonic::Result<tonic::Response<Self::StreamDataStream>, tonic::Status> {
        let request = request.into_inner();
        info!(request = ?request, "stream data request");
        todo!();
    }
}

impl ChainViewExt for ChainView {
    async fn get_status(&self) -> Result<StatusResponse, ChainViewError> {
        let starting = self.get_starting_cursor().await?;
        let finalized = self.get_finalized_cursor().await?;
        let head = self.get_head().await?;

        Ok(StatusResponse {
            current_head: None,
            last_ingested: Some(head.into()),
            finalized: Some(finalized.into()),
            starting: Some(starting.into()),
        })
    }
}
