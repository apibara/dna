//! Stream gRPC server.

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use apibara_core::{
    application::pb as app_pb,
    node::v1alpha1::pb as node_pb,
    stream::{Sequence, StreamMessage},
};
use futures::Stream;
use opentelemetry::metrics::ObservableCounter;
use prost::DecodeError;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tower_http::trace::{OnFailure, OnRequest, TraceLayer};
use tracing::{warn, Span};

use crate::{
    o11y, processor::MessageProducer, reflection::merge_encoded_node_service_descriptor_set,
};

lazy_static::lazy_static! {
    static ref DEFAULT_ADDRESS: SocketAddr = "0.0.0.0:7171".parse().unwrap();
}

pub struct Server<P: MessageProducer> {
    producer: Arc<P>,
    address: SocketAddr,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("grpc server error")]
    Transport(#[from] tonic::transport::Error),
    #[error("error starting reflection server")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
    #[error("error decoding protobuf message")]
    Decode(#[from] DecodeError),
}

impl<P> Server<P>
where
    P: MessageProducer,
{
    /// Creates a new stream server.
    pub fn new(producer: Arc<P>) -> Self {
        Server {
            producer,
            address: *DEFAULT_ADDRESS,
        }
    }

    /// Changes the server address.
    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = address;
        self
    }

    /// Starts the grpc server.
    pub async fn start(
        self,
        output: &app_pb::OutputStream,
        ct: CancellationToken,
    ) -> Result<(), ServerError> {
        let server = ServerImpl::new(self.producer, output.message_type.clone(), ct.clone());

        let node_descriptor_set = merge_encoded_node_service_descriptor_set(
            &output.filename,
            &output.file_descriptor_proto,
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

        TonicServer::builder()
            .layer(tracing_layer)
            .add_service(server.into_service())
            .add_service(reflection_service)
            .serve_with_shutdown(self.address, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            })
            .await?;

        Ok(())
    }
}

struct ServerImpl<P: MessageProducer> {
    producer: Arc<P>,
    type_url: String,
    cts: CancellationToken,
}

impl<P> ServerImpl<P>
where
    P: MessageProducer,
{
    pub fn new(producer: Arc<P>, output_type: String, cts: CancellationToken) -> Self {
        let type_url = format!("type.googleapis.com/{}", output_type);
        ServerImpl {
            producer,
            type_url,
            cts,
        }
    }

    pub fn into_service(self) -> node_pb::node_server::NodeServer<ServerImpl<P>> {
        node_pb::node_server::NodeServer::new(self)
    }
}

type TonicResult<T> = std::result::Result<Response<T>, Status>;

#[tonic::async_trait]
impl<P> node_pb::node_server::Node for ServerImpl<P>
where
    P: MessageProducer,
{
    async fn status(
        &self,
        _request: Request<node_pb::StatusRequest>,
    ) -> TonicResult<node_pb::StatusResponse> {
        todo!()
    }

    type StreamMessagesStream = Pin<
        Box<
            dyn Stream<Item = std::result::Result<node_pb::StreamMessagesResponse, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn stream_messages(
        &self,
        request: Request<node_pb::StreamMessagesRequest>,
    ) -> TonicResult<Self::StreamMessagesStream> {
        let request: node_pb::StreamMessagesRequest = request.into_inner();

        let starting_sequence = Sequence::from_u64(request.starting_sequence);
        let pending_interval = if request.pending_block_interval_seconds == 0 {
            None
        } else {
            let interval = Duration::from_secs(request.pending_block_interval_seconds as u64);
            Some(interval)
        };

        let stream = self
            .producer
            .stream_from_sequence(&starting_sequence, pending_interval, self.cts.clone())
            .map_err(|_| Status::internal("failed to start message stream"))?;

        let response = Box::pin(stream.map({
            let type_url = self.type_url.clone();
            move |maybe_res| match maybe_res {
                Err(err) => {
                    warn!(err = ?err, "stream error");
                    Err(Status::internal("stream error"))
                }
                Ok(StreamMessage::Invalidate { sequence }) => {
                    let invalidate = node_pb::Invalidate {
                        sequence: sequence.as_u64(),
                    };
                    Ok(node_pb::StreamMessagesResponse {
                        message: Some(node_pb::stream_messages_response::Message::Invalidate(
                            invalidate,
                        )),
                    })
                }
                Ok(StreamMessage::Data { sequence, data }) => {
                    let inner_data = prost_types::Any {
                        type_url: type_url.clone(),
                        value: data.as_bytes().to_vec(),
                    };
                    let data = node_pb::Data {
                        sequence: sequence.as_u64(),
                        data: Some(inner_data),
                    };

                    Ok(node_pb::StreamMessagesResponse {
                        message: Some(node_pb::stream_messages_response::Message::Data(data)),
                    })
                }
                Ok(StreamMessage::Pending { sequence, data }) => {
                    let inner_data = prost_types::Any {
                        type_url: type_url.clone(),
                        value: data.as_bytes().to_vec(),
                    };
                    let pending = node_pb::Data {
                        sequence: sequence.as_u64(),
                        data: Some(inner_data),
                    };

                    Ok(node_pb::StreamMessagesResponse {
                        message: Some(node_pb::stream_messages_response::Message::Pending(pending)),
                    })
                }
            }
        }));

        Ok(Response::new(response))
    }
}

#[derive(Clone)]
struct Metrics {
    rpc_call_count: Arc<ObservableCounter<u64>>,
    rpc_error_count: Arc<ObservableCounter<u64>>,
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
