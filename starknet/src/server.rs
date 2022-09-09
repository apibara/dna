//! gRPC server.

use std::{net::SocketAddr, pin::Pin, sync::Arc};

use apibara_core::pb;
use apibara_node::{
    chain_tracker::ChainTracker,
    db::libmdbx::{Environment, EnvironmentKind},
    reflection::merge_encoded_node_service_descriptor_set,
};
use futures::{Stream, StreamExt};
use prost::{DecodeError, Message};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tracing::{error, info, warn};

use crate::{
    block_ingestion::{BlockIngestor, BlockStreamMessage},
    core::{starknet_file_descriptor_set, Block},
    health_reporter::HealthReporter,
    status_reporter::StatusReporter,
};

type TonicResult<T> = std::result::Result<Response<T>, Status>;

pub struct NodeServer<E: EnvironmentKind> {
    status_reporter: StatusReporter<E>,
    block_ingestor: Arc<BlockIngestor<E>>,
}

impl<E> NodeServer<E>
where
    E: EnvironmentKind,
{
    pub fn new(chain: Arc<ChainTracker<Block, E>>, block_ingestor: Arc<BlockIngestor<E>>) -> Self {
        let status_reporter = StatusReporter::new(chain);
        NodeServer {
            block_ingestor,
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
        request: Request<pb::ConnectRequest>,
    ) -> TonicResult<Self::ConnectStream> {
        let request: pb::ConnectRequest = request.into_inner();

        let stream = self
            .block_ingestor
            .stream_from_sequence(request.starting_sequence)
            .map_err(|_| Status::internal("failed to stream backfilled blocks"))?;

        let response = Box::pin(stream.map(|maybe_res| match maybe_res {
            Err(err) => {
                warn!(err = ?err, "stream failed");
                Err(Status::internal("stream failed"))
            }
            Ok(BlockStreamMessage::Reorg(sequence)) => {
                let invalidate = pb::Invalidate { sequence };
                Ok(pb::ConnectResponse {
                    message: Some(pb::connect_response::Message::Invalidate(invalidate)),
                })
            }
            Ok(BlockStreamMessage::Data(block)) => {
                let inner_data = prost_types::Any {
                    type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Block".to_string(),
                    value: block.encode_to_vec(),
                };
                let data = pb::Data {
                    sequence: block.block_number,
                    data: Some(inner_data),
                };

                Ok(pb::ConnectResponse {
                    message: Some(pb::connect_response::Message::Data(data)),
                })
            }
        }));

        Ok(Response::new(response))
    }
}

pub struct Server<E: EnvironmentKind> {
    db: Arc<Environment<E>>,
    chain: Arc<ChainTracker<Block, E>>,
    block_ingestor: Arc<BlockIngestor<E>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("grpc transport error")]
    Transport(#[from] tonic::transport::Error),
    #[error("error building reflection server")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
    #[error("error decoding file descriptor set")]
    Prost(#[from] DecodeError),
    #[error("error awaiting task")]
    Task(#[from] JoinError),
}

pub type Result<T> = std::result::Result<T, ServerError>;

impl<E> Server<E>
where
    E: EnvironmentKind,
{
    pub fn new(
        db: Arc<Environment<E>>,
        chain: Arc<ChainTracker<Block, E>>,
        block_ingestor: Arc<BlockIngestor<E>>,
    ) -> Self {
        Server {
            db,
            chain,
            block_ingestor,
        }
    }

    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<()> {
        let node_server = NodeServer::new(self.chain, self.block_ingestor);

        let (mut health_reporter, health_service) = HealthReporter::new(self.db.clone());

        let reporter_handle = tokio::spawn({
            let ct = ct.clone();
            async move { health_reporter.start(ct).await }
        });

        /*
        let starknet_set = prost_types::FileDescriptorSet::decode(starknet_file_descriptor_set()).unwrap();
        for f in &starknet_set.file {
            println!("file = {:?}", f.name());
            println!("dep = {:?}", f.dependency);
            println!("pub = {:?}", f.public_dependency);
        }

        println!("node");
        let mut node_set = prost_types::FileDescriptorSet::decode(node_file_descriptor_set()).unwrap();
        node_set.file[1].dependency.push("starknet.proto".to_string());
        for f in &node_set.file {
            println!("file = {:?}", f.name());
            println!("dep = {:?}", f.dependency);
            println!("pub = {:?}", f.public_dependency);
        }

        let mut final_file = Vec::default();
        final_file.extend(starknet_set.file.into_iter());
        final_file.extend(node_set.file.into_iter());
        let final_set = prost_types::FileDescriptorSet {
            file: final_file
        };
        println!("final");
        for f in &final_set.file {
            println!("file = {:?}", f.name());
            println!("dep = {:?}", f.dependency);
            println!("pub = {:?}", f.public_dependency);
        }
        */
        let node_descriptor_set = merge_encoded_node_service_descriptor_set(
            "starknet.proto",
            starknet_file_descriptor_set(),
        )?;

        let reflection_service = tonic_reflection::server::Builder::configure()
            //.register_encoded_file_descriptor_set(starknet_file_descriptor_set())
            //.register_encoded_file_descriptor_set(node_file_descriptor_set())
            .register_file_descriptor_set(node_descriptor_set)
            .register_encoded_file_descriptor_set(
                tonic_health::proto::GRPC_HEALTH_V1_FILE_DESCRIPTOR_SET,
            )
            .build()?;

        info!(addr = ?addr, "starting server");
        TonicServer::builder()
            .add_service(node_server.into_service())
            .add_service(health_service)
            .add_service(reflection_service)
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
