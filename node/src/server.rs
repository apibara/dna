//! Stream gRPC server.

use std::{marker::PhantomData, net::SocketAddr, pin::Pin, sync::Arc};

use apibara_core::{node::pb as node_pb, stream::Sequence};
use futures::Stream;
use tokio_util::sync::CancellationToken;
use tokio_stream::StreamExt;
use tonic::{transport::Server as TonicServer, IntoRequest, Request, Response, Status};
use tracing::warn;

use crate::{processor::MessageProducer, reflection::merge_encoded_node_service_descriptor_set};

pub struct Server<P: MessageProducer> {
    producer: Arc<P>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("grpc server error")]
    Transport(#[from] tonic::transport::Error),
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
    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<(), ServerError> {
        let server = ServerImpl::new(self.producer, ct.clone());

        TonicServer::builder()
            .add_service(server.into_service())
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
            Ok(val) => {
                println!("val = {:?}", val);
                Err(Status::internal("stream error"))
            }
        }));

        Ok(Response::new(response))
    }
}
