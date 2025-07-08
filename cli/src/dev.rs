use std::{net::SocketAddr, sync::Arc};

use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{AdminService, InMemoryAdminService};

use crate::error::{CliError, CliResult};

#[derive(Debug, Args)]
pub struct DevArgs {
    #[arg(long, default_value = "127.0.0.1:7777")]
    bind: String,
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> CliResult<()> {
        let address =
            self.bind
                .parse::<SocketAddr>()
                .change_context(CliError::InvalidConfiguration {
                    message: "failed to parse address".to_string(),
                })?;

        println!("Starting Wings in development mode on {}", address);

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                wings_metadata_core::protocol::admin_file_descriptor_set(),
            )
            .build_v1()
            .change_context(CliError::Service {
                message: "failed to create tonic reflection service".to_string(),
            })?;

        let admin = Arc::new(InMemoryAdminService::default());
        let admin_service = AdminService::new(admin).into_service();

        let server = tonic::transport::Server::builder()
            .add_service(reflection_service)
            .add_service(admin_service)
            .serve_with_shutdown(address, async move {
                ct.cancelled().await;
            });

        server.await.change_context(CliError::Server {
            message: "error while running grpc dev server".to_string(),
        })
    }
}
