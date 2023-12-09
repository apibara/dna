mod client;
mod server;
mod service;

use std::{fmt, net::SocketAddr, pin::Pin};

use apibara_sdk::StreamClient;
use error_stack::{Result, ResultExt};
use futures::Future;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

use self::{
    server::{proto::sink_file_descriptor_set, Server},
    service::StatusService,
};

pub use self::client::StatusServerClient;
pub use self::server::proto::{status_client::StatusClient, GetStatusRequest, GetStatusResponse};

#[derive(Clone)]
pub struct StatusServer {
    address: SocketAddr,
}

#[derive(Debug)]
pub struct StatusServerError;
impl error_stack::Context for StatusServerError {}

impl fmt::Display for StatusServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("status server operation failed")
    }
}

impl StatusServer {
    pub fn new(address: SocketAddr) -> Self {
        StatusServer { address }
    }

    /// Starts the status server.
    pub async fn start(
        self,
        stream_client: StreamClient,
        ct: CancellationToken,
    ) -> Result<
        (
            StatusServerClient,
            Pin<Box<impl Future<Output = Result<(), StatusServerError>>>>,
        ),
        StatusServerError,
    > {
        let (status_service, status_client, status_service_client, health_server) =
            StatusService::new();
        let status_server = Server::new(status_service_client, stream_client);

        let status_fut = Box::pin({
            let address = self.address;
            async move {
                let status_service_fut = status_service.start(ct.clone());

                let listener = TcpListener::bind(address)
                    .await
                    .change_context(StatusServerError)
                    .attach_printable("failed to bind status server")?;

                let local_addr = listener
                    .local_addr()
                    .change_context(StatusServerError)
                    .attach_printable("failed to get local address")?;
                info!("status server listening on {}", local_addr);
                let listener = TcpListenerStream::new(listener);

                let reflection_service = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(sink_file_descriptor_set())
                    .build()
                    .change_context(StatusServerError)
                    .attach_printable("failed to register to gRPC reflection service")?;

                let server_fut = TonicServer::builder()
                    .add_service(health_server)
                    .add_service(status_server.into_service())
                    .add_service(reflection_service)
                    .serve_with_incoming_shutdown(listener, {
                        let ct = ct.clone();
                        async move { ct.cancelled().await }
                    });

                tokio::select! {
                    server_ret = server_fut => {
                        match server_ret {
                            Ok(_) => {},
                            Err(err) => {
                                return Err(err)
                                    .change_context(StatusServerError)
                                    .attach_printable("status server stopped: grpc");
                            }
                        }
                    }
                    status_ret = status_service_fut => {
                        match status_ret {
                            Ok(_) => {},
                            Err(err) => {
                                return Err(err)
                                    .change_context(StatusServerError)
                                    .attach_printable("status server stopped: status service");
                            }
                        }
                    }
                }

                Ok(())
            }
        });

        Ok((status_client, status_fut))
    }
}
