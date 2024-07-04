use std::net::SocketAddr;

use apibara_dna_protocol::dna::stream::{dna_stream_file_descriptor_set, dna_stream_server};
use error_stack::{Result, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server as TonicServer;
use tracing::info;

#[derive(Debug)]
pub struct DnaServerError;

pub struct DnaServer<S>
where
    S: dna_stream_server::DnaStream,
{
    service: S,
}

pub type TonicServerResult = ::std::result::Result<(), tonic::transport::Error>;

impl<S> DnaServer<S>
where
    S: dna_stream_server::DnaStream,
{
    pub fn new(service: S) -> Self {
        Self { service }
    }

    pub async fn start(
        self,
        addr: SocketAddr,
        ct: CancellationToken,
    ) -> Result<JoinHandle<TonicServerResult>, DnaServerError> {
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dna_stream_file_descriptor_set())
            .build()
            .change_context(DnaServerError)
            .attach_printable("failed to create gRPC reflection service")?;

        let service = dna_stream_server::DnaStreamServer::new(self.service);

        info!(addr = %addr, "starting dna server");
        let server_task = TonicServer::builder()
            .add_service(reflection_service)
            .add_service(service)
            .serve_with_shutdown(addr, {
                let ct = ct.clone();
                async move {
                    ct.cancelled().await;
                }
            });

        Ok(tokio::spawn(server_task))
    }
}

impl error_stack::Context for DnaServerError {}

impl std::fmt::Display for DnaServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DNA server error")
    }
}
