//! Stream gRPC server.

use std::{net::SocketAddr, pin::Pin, sync::Arc};

use apibara_core::{
    application::pb as app_pb,
    node::pb as node_pb,
    stream::{Sequence, StreamMessage},
};
use futures::Stream;
use prost::DecodeError;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tracing::warn;

use crate::{processor::MessageProducer, reflection::merge_encoded_node_service_descriptor_set};

pub struct Server<P: MessageProducer> {
    producer: Arc<P>,
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
        Server { producer }
    }

    /// Starts the grpc server.
    pub async fn start(
        self,
        addr: SocketAddr,
        output: &app_pb::OutputStream,
        ct: CancellationToken,
    ) -> Result<(), ServerError> {
        let server = ServerImpl::new(self.producer, ct.clone());

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

        TonicServer::builder()
            .add_service(server.into_service())
            .add_service(reflection_service)
            .serve_with_shutdown(addr, {
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
    cts: CancellationToken,
}

impl<P> ServerImpl<P>
where
    P: MessageProducer,
{
    pub fn new(producer: Arc<P>, cts: CancellationToken) -> Self {
        ServerImpl { producer, cts }
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
        let stream = self
            .producer
            .stream_from_sequence(&starting_sequence, self.cts.clone())
            .map_err(|_| Status::internal("failed to start message stream"))?;

        let response = Box::pin(stream.map(|maybe_res| match maybe_res {
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
                    type_url: "type.googleapis.com/apibara.starknet.v1alpha1.Event".to_string(),
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
        }));

        Ok(Response::new(response))
    }
}
