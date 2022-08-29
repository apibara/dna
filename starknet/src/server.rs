//! gRPC server.

use std::{net::SocketAddr, pin::Pin};

use apibara_core::pb::{self, ConnectResponse};
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::core::Block;

type TonicResult<T> = std::result::Result<Response<T>, Status>;

pub struct NodeServer {
    block_rx: Receiver<Block>,
}

impl NodeServer {
    pub fn new(block_rx: Receiver<Block>) -> Self {
        NodeServer { block_rx }
    }

    pub fn into_service(self) -> pb::node_server::NodeServer<NodeServer> {
        pb::node_server::NodeServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::node_server::Node for NodeServer {
    async fn health(&self, request: Request<pb::HealthRequest>) -> TonicResult<pb::HealthResponse> {
        todo!()
    }

    async fn status(&self, request: Request<pb::StatusRequest>) -> TonicResult<pb::StatusResponse> {
        todo!()
    }

    type ConnectStream = Pin<
        Box<dyn Stream<Item = std::result::Result<pb::ConnectResponse, Status>> + Send + 'static>,
    >;

    async fn connect(
        &self,
        _request: Request<Streaming<pb::ConnectRequest>>,
    ) -> TonicResult<Self::ConnectStream> {
        let rx = self.block_rx.resubscribe();

        let stream = BroadcastStream::new(rx);
        let response_stream = Box::pin(stream.map(|maybe_block| match maybe_block {
            Err(err) => {
                error!(err = ?err, "connect stream error");
                Err(Status::internal("internal error"))
            }
            Ok(block) => {
                let mut buf = Vec::new();
                buf.reserve(block.encoded_len());
                block
                    .encode(&mut buf)
                    .map_err(|_| Status::internal("error encoding block data"))?;
                let inner_data = prost_types::Any {
                    type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Block".to_string(),
                    value: buf,
                };
                let data = pb::Data {
                    sequence: block.block_number,
                    data: Some(inner_data),
                };
                let message = pb::connect_response::Message::Data(data);
                Ok(pb::ConnectResponse {
                    message: Some(message),
                })
            }
        }));
        Ok(Response::new(response_stream))
    }
}

pub struct Server {}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("grpc transport error")]
    Transport(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, ServerError>;

impl Server {
    pub fn new() -> Self {
        Server {}
    }

    pub async fn start(
        &self,
        addr: SocketAddr,
        block_rx: Receiver<Block>,
        ct: CancellationToken,
    ) -> Result<()> {
        let node_server = NodeServer::new(block_rx);

        info!(addr = ?addr, "starting server");
        TonicServer::builder()
            .add_service(node_server.into_service())
            .serve_with_shutdown(addr, {
                async move {
                    ct.cancelled().await;
                }
            })
            .await?;

        Ok(())
    }
}
