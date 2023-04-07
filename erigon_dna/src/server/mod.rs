use std::{net::SocketAddr, path::PathBuf};

use apibara_core::node as node_pb;
use apibara_node::server::{RequestObserver, SimpleRequestObserver};
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::{error, info, info_span};

use crate::server::stream::StreamService;

mod stream;

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("grpc transport error")]
    Transport(#[from] tonic::transport::Error),
    #[error("error awaiting task")]
    Task(#[from] JoinError),
    #[error("error starting reflection server")]
    ReflectionServer(#[from] tonic_reflection::server::Error),
}

pub struct Server<O: RequestObserver> {
    private_api_url: String,
    datadir: PathBuf,
    request_observer: O,
}

impl Server<SimpleRequestObserver> {
    pub fn new(private_api_url: String, datadir: PathBuf) -> Self {
        let request_observer = SimpleRequestObserver::default();
        Server {
            private_api_url,
            datadir,
            request_observer,
        }
    }
}

impl<O> Server<O>
where
    O: RequestObserver,
{
    pub async fn start(self, addr: SocketAddr, ct: CancellationToken) -> Result<(), ServerError> {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(node_pb::v1alpha2::node_file_descriptor_set())
            .build()?;

        let stream_service =
            StreamService::new(self.private_api_url, &self.datadir, self.request_observer)
                .into_service();

        info!(addr = %addr, "starting server");

        TonicServer::builder()
            .trace_fn(|_| info_span!("node_server"))
            .add_service(stream_service)
            .add_service(reflection_service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move { ct.cancelled().await }
            })
            .await?;

        // signal health reporter to stop and wait for it
        ct.cancel();

        Ok(())
    }
}
