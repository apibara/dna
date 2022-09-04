//! gRPC server.

use std::{net::SocketAddr, pin::Pin, sync::Arc};

use apibara_core::pb;
use apibara_node::{
    chain_tracker::ChainTracker,
    db::libmdbx::{Environment, EnvironmentKind},
};
use futures::{Stream, StreamExt};
use prost::Message;
use tokio::{sync::broadcast::Receiver, task::JoinError};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{core::Block, health_reporter::HealthReporter, status_reporter::StatusReporter};

type TonicResult<T> = std::result::Result<Response<T>, Status>;

pub struct NodeServer<E: EnvironmentKind> {
    status_reporter: StatusReporter<E>,
    block_rx: Receiver<Block>,
}

impl<E> NodeServer<E>
where
    E: EnvironmentKind,
{
    pub fn new(chain: Arc<ChainTracker<Block, E>>, block_rx: Receiver<Block>) -> Self {
        let status_reporter = StatusReporter::new(chain);
        NodeServer {
            block_rx,
            status_reporter,
        }
    }

    pub fn into_service(self) -> pb::node_server::NodeServer<NodeServer<E>> {
        pb::node_server::NodeServer::new(self)
    }
}

#[tonic::async_trait]
impl<E> pb::node_server::Node for NodeServer<E>
where
    E: EnvironmentKind,
{
    async fn status(
        &self,
        _request: Request<pb::StatusRequest>,
    ) -> TonicResult<pb::StatusResponse> {
        let status = self
            .status_reporter
            .status()
            .map_err(|_| Status::internal("failed to compute status"))?;
        Ok(Response::new(status))
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

pub struct Server<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    chain: Arc<ChainTracker<Block, E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("grpc transport error")]
    Transport(#[from] tonic::transport::Error),
    #[error("error awaiting task")]
    Task(#[from] JoinError),
}

pub type Result<T> = std::result::Result<T, ServerError>;

impl<E> Server<E>
where
    E: EnvironmentKind,
{
    pub fn new(db: Arc<Environment<E>>, chain: Arc<ChainTracker<Block, E>>) -> Self {
        Server { db, chain }
    }

    pub async fn start(
        self,
        addr: SocketAddr,
        block_rx: Receiver<Block>,
        ct: CancellationToken,
    ) -> Result<()> {
        let node_server = NodeServer::new(self.chain, block_rx);

        let (mut health_reporter, health_service) = HealthReporter::new(self.db.clone());

        let reporter_handle = tokio::spawn({
            let ct = ct.clone();
            async move { health_reporter.start(ct).await }
        });

        info!(addr = ?addr, "starting server");
        TonicServer::builder()
            .add_service(node_server.into_service())
            .add_service(health_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            })
            .await?;

        // signal health reporter to stop
        ct.cancel();

        reporter_handle.await?;

        Ok(())
    }
}
