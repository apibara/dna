use std::{net::SocketAddr, sync::Arc};

use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{
    Admin, AdminService, InMemoryAdminService, NamespaceName, NamespaceOptions, SecretName,
    TenantName,
};

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

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(
                wings_metadata_core::protocol::admin_file_descriptor_set(),
            )
            .build_v1()
            .change_context(CliError::Service {
                message: "failed to create tonic reflection service".to_string(),
            })?;

        let admin = Arc::new(InMemoryAdminService::default());

        let default_tenant = TenantName::new_unchecked("default");
        admin
            .create_tenant(default_tenant.clone())
            .await
            .expect("failed to create default tenant");

        let default_namespace = NamespaceName::new_unchecked("default", default_tenant);
        let default_namespace_options = NamespaceOptions::new(SecretName::new_unchecked("defaul"));
        admin
            .create_namespace(default_namespace.clone(), default_namespace_options)
            .await
            .expect("failed to create default namespace");

        let admin_service = AdminService::new(admin).into_service();

        println!("Starting Wings in development mode on {}", address);
        println!("Default namespace: {}", default_namespace);

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
