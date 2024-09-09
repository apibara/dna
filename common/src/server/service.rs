use std::pin::Pin;

use apibara_dna_protocol::dna::stream::{
    dna_stream_server::{self, DnaStream},
    StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
};
use error_stack::ResultExt;
use futures::Stream;
use tracing::info;

use crate::chain_view::ChainView;

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

#[tonic::async_trait]
impl DnaStream for StreamService {
    type StreamDataStream =
        Pin<Box<dyn Stream<Item = Result<StreamDataResponse, tonic::Status>> + Send + 'static>>;

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> tonic::Result<tonic::Response<StatusResponse>, tonic::Status> {
        let Some(chain_view) = self.chain_view.borrow().clone() else {
            return Err(tonic::Status::unavailable("chain view not initialized yet"));
        };

        let sb = chain_view
            .get_finalized_cursor()
            .await
            .map_err(|_| tonic::Status::unavailable("chain view not initialized yet"))?;
        println!("sb: {:?}", sb);
        todo!();
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
