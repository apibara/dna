//! gRPC server.

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use apibara_core::node::v1alpha1::pb;
use apibara_node::{
    chain_tracker::ChainTracker,
    db::libmdbx::{Environment, EnvironmentKind},
    heartbeat::HeartbeatStreamExt,
    o11y::{self, ObservableCounter},
    reflection::merge_encoded_node_service_descriptor_set,
};
use futures::Stream;
use prost::DecodeError;
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tower_http::trace::{OnFailure, OnRequest, TraceLayer};
use tracing::{debug, error, info, info_span, warn, Span};

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
    heartbeat_interval: Duration,
    cts: CancellationToken,
}

impl<E> NodeServer<E>
where
    E: EnvironmentKind,
{
    pub fn new(
        chain: Arc<ChainTracker<Block, E>>,
        block_ingestor: Arc<BlockIngestor<E>>,
        cts: CancellationToken,
    ) -> Self {
        let status_reporter = StatusReporter::new(chain);
        // This value should come from config.
        let heartbeat_interval = Duration::from_secs(30);

        NodeServer {
            block_ingestor,
            status_reporter,
            heartbeat_interval,
            cts,
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
        info!("received status request");
        let status = self
            .status_reporter
            .status()
            .map_err(|_| Status::internal("failed to compute status"))?;
        Ok(Response::new(status))
    }

    type StreamMessagesStream = Pin<
        Box<
            dyn Stream<Item = std::result::Result<pb::StreamMessagesResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn stream_messages(
        &self,
        request: Request<pb::StreamMessagesRequest>,
    ) -> TonicResult<Self::StreamMessagesStream> {
        let request: pb::StreamMessagesRequest = request.into_inner();

        info!(starting_sequence = %request.starting_sequence, "stream messages");

        let pending_interval = if request.pending_block_interval_seconds == 0 {
            None
        } else {
            let interval = Duration::from_secs(request.pending_block_interval_seconds as u64);
            Some(interval)
        };

        let stream = self
            .block_ingestor
            .stream_from_sequence(
                request.starting_sequence,
                pending_interval,
                self.cts.clone(),
            )
            .map_err(|_| Status::internal("failed to start message stream"))?;

        let response =
            Box::pin(
                stream
                    .heartbeat(self.heartbeat_interval)
                    .map(|maybe_res| match maybe_res {
                        Err(_) => {
                            debug!("sending heartbeat");
                            let heartbeat = pb::Heartbeat {};
                            Ok(pb::StreamMessagesResponse {
                                message: Some(pb::stream_messages_response::Message::Heartbeat(
                                    heartbeat,
                                )),
                            })
                        }
                        Ok(Err(err)) => {
                            warn!(err = ?err, "stream failed");
                            Err(Status::internal("stream failed"))
                        }
                        Ok(Ok(BlockStreamMessage::Invalidate { sequence })) => {
                            let invalidate = pb::Invalidate {
                                sequence: sequence.as_u64(),
                            };
                            Ok(pb::StreamMessagesResponse {
                                message: Some(pb::stream_messages_response::Message::Invalidate(
                                    invalidate,
                                )),
                            })
                        }
                        Ok(Ok(BlockStreamMessage::Data {
                            data: block,
                            sequence,
                        })) => {
                            let inner_data = prost_types::Any {
                                type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Block"
                                    .to_string(),
                                value: block.as_bytes().to_vec(),
                            };

                            let data = pb::Data {
                                sequence: sequence.as_u64(),
                                data: Some(inner_data),
                            };

                            Ok(pb::StreamMessagesResponse {
                                message: Some(pb::stream_messages_response::Message::Data(data)),
                            })
                        }
                        Ok(Ok(BlockStreamMessage::Pending {
                            data: block,
                            sequence,
                        })) => {
                            let inner_data = prost_types::Any {
                                type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Block"
                                    .to_string(),
                                value: block.as_bytes().to_vec(),
                            };
                            let pending = pb::Data {
                                sequence: sequence.as_u64(),
                                data: Some(inner_data),
                            };

                            Ok(pb::StreamMessagesResponse {
                                message: Some(pb::stream_messages_response::Message::Pending(
                                    pending,
                                )),
                            })
                        }
                    }),
            );

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

#[derive(Clone)]
struct Metrics {
    rpc_call_count: Arc<ObservableCounter<u64>>,
    rpc_error_count: Arc<ObservableCounter<u64>>,
}

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
        let node_server = NodeServer::new(self.chain, self.block_ingestor, ct.clone());

        let (mut health_reporter, health_service) = HealthReporter::new(self.db.clone());

        let reporter_handle = tokio::spawn({
            let ct = ct.clone();
            async move { health_reporter.start(ct).await }
        });

        let node_descriptor_set = merge_encoded_node_service_descriptor_set(
            "starknet.proto",
            starknet_file_descriptor_set(),
        )?;

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_file_descriptor_set(node_descriptor_set)
            .register_encoded_file_descriptor_set(
                tonic_health::proto::GRPC_HEALTH_V1_FILE_DESCRIPTOR_SET,
            )
            .build()?;

        let metrics = Metrics::new();

        let tracing_layer = tower::ServiceBuilder::new()
            .layer(TraceLayer::new_for_grpc().on_request(metrics))
            .into_inner();

        info!(addr = ?addr, "starting server");
        TonicServer::builder()
            .trace_fn(|_| info_span!("node_server"))
            .layer(tracing_layer)
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

impl Metrics {
    pub fn new() -> Self {
        let meter = o11y::meter("apibara.com/starknet");
        let rpc_call_count = meter
            .u64_observable_counter("rpc_call_count")
            .with_description("Number of rpc calls")
            .init();
        let rpc_error_count = meter
            .u64_observable_counter("rpc_error_count")
            .with_description("Number of rpc calls that returned an error")
            .init();
        Metrics {
            rpc_call_count: Arc::new(rpc_call_count),
            rpc_error_count: Arc::new(rpc_error_count),
        }
    }
}

impl OnRequest<hyper::Body> for Metrics {
    fn on_request(&mut self, request: &hyper::Request<hyper::Body>, _span: &Span) {
        let cx = o11y::Context::current();
        let path = request.uri().path().to_string();
        self.rpc_call_count
            .observe(&cx, 1, &[o11y::KeyValue::new("grpc.path", path)]);
    }
}

impl OnFailure<hyper::Body> for Metrics {
    fn on_failure(
        &mut self,
        _failure_classification: hyper::Body,
        _latency: std::time::Duration,
        _span: &Span,
    ) {
        let cx = o11y::Context::current();
        self.rpc_error_count.observe(&cx, 1, &[]);
    }
}
