use apibara_sdk::StreamClient;
use tonic::async_trait;

use super::service::StatusServiceClient;

pub mod proto {
    tonic::include_proto!("apibara.sink.v1");
    pub const SINK_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sink_v1alpha2_descriptor");

    pub fn sink_file_descriptor_set() -> &'static [u8] {
        SINK_DESCRIPTOR_SET
    }
}

pub type StatusServer = proto::status_server::StatusServer<Server>;

pub struct Server {
    client: StatusServiceClient,
    stream_client: StreamClient,
}

impl Server {
    pub fn new(client: StatusServiceClient, stream_client: StreamClient) -> Self {
        Self {
            client,
            stream_client,
        }
    }

    pub fn into_service(self) -> proto::status_server::StatusServer<Self> {
        proto::status_server::StatusServer::new(self)
    }
}

#[async_trait]
impl proto::status_server::Status for Server {
    async fn get_status(
        &self,
        _request: tonic::Request<proto::GetStatusRequest>,
    ) -> Result<tonic::Response<proto::GetStatusResponse>, tonic::Status> {
        let cursors = self
            .client
            .get_cursors()
            .await
            .map_err(|_| tonic::Status::internal("failed to get sink cursors"))?;

        let dna_status = self
            .stream_client
            .clone()
            .status()
            .await
            .map_err(|_| tonic::Status::internal("failed to get status from dna server"))?;

        let response = proto::GetStatusResponse {
            status: proto::SinkStatus::Running as i32,
            starting_block: cursors.starting.map(|cursor| cursor.order_key),
            current_block: cursors.current.map(|cursor| cursor.order_key),
            head_block: dna_status.current_head.map(|cursor| cursor.order_key),
            reason: None,
        };

        Ok(tonic::Response::new(response))
    }
}
