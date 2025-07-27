use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use clap::Args;
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_ingestor_core::{BatchIngestor, ingestor::BatchIngestorClient, run_background_ingestor};
use wings_ingestor_http::HttpIngestor;
use wings_metadata_core::{
    admin::{
        Admin, AdminService, InMemoryAdminService, NamespaceName, NamespaceOptions, SecretName,
        TenantName,
    },
    cache::{NamespaceCache, TopicCache},
    offset_registry::{InMemoryOffsetRegistry, server::OffsetRegistryService},
};
use wings_object_store::TemporaryFileSystemFactory;
use wings_server_http::HttpServer;

use crate::error::{CliError, CliResult};

#[derive(Debug, Args)]
pub struct DevArgs {
    /// The address of the gRPC metadata server.
    #[arg(long, default_value = "127.0.0.1:7777")]
    metadata_address: String,
    /// The address of the HTTP ingestor server.
    #[arg(long, default_value = "127.0.0.1:7780")]
    http_address: String,
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> CliResult<()> {
        let (admin, default_namespace) = new_dev_admin_service().await;

        let metadata_address = self.metadata_address.parse::<SocketAddr>().change_context(
            CliError::InvalidConfiguration {
                message: "failed to parse metadata address".to_string(),
            },
        )?;

        let http_address = self.http_address.parse::<SocketAddr>().change_context(
            CliError::InvalidConfiguration {
                message: "failed to parse http address".to_string(),
            },
        )?;

        println!("Starting Wings in development mode");
        println!("Default namespace: {}", default_namespace);
        println!("gRPC server listening on {}", metadata_address);
        println!("HTTP ingestor listening on {}", http_address);

        {
            let _ct_guard = ct.child_token().drop_guard();
            let object_store_factory =
                TemporaryFileSystemFactory::new().change_context(CliError::Server {
                    message: "error creating local temporary directory".to_string(),
                })?;

            let offset_registry = Arc::new(InMemoryOffsetRegistry::new());

            println!(
                "Object store root path: {}",
                object_store_factory.root_path().display()
            );

            let ingestor =
                BatchIngestor::new(Arc::new(object_store_factory), offset_registry.clone());

            let grpc_server_fut = run_grpc_server(
                admin.clone(),
                offset_registry,
                ingestor.client(),
                metadata_address,
                ct.clone(),
            );
            let http_ingestor_fut =
                run_http_ingestor(admin, ingestor.client(), http_address, ct.clone());

            let ingestor_fut = run_background_ingestor(ingestor, ct);

            tokio::select! {
                res = grpc_server_fut => {
                    println!("gRPC server exited with {:?}", res);
                },
                res = http_ingestor_fut => {
                    println!("HTTP ingestor server exited with {:?}", res);
                },
                res = ingestor_fut => {
                    println!("Background ingestor exited with {:?}", res);
                },
            }
        }

        Ok(())
    }
}

async fn new_dev_admin_service() -> (Arc<InMemoryAdminService>, NamespaceName) {
    let admin = Arc::new(InMemoryAdminService::default());

    let default_tenant = TenantName::new_unchecked("default");
    admin
        .create_tenant(default_tenant.clone())
        .await
        .expect("failed to create default tenant");

    let default_namespace = NamespaceName::new_unchecked("default", default_tenant);
    let default_namespace_options =
        NamespaceOptions::new(SecretName::new_unchecked("default-bucket"));
    admin
        .create_namespace(default_namespace.clone(), default_namespace_options)
        .await
        .expect("failed to create default namespace");

    (admin, default_namespace)
}

async fn run_grpc_server(
    admin: Arc<InMemoryAdminService>,
    offset_registry: Arc<InMemoryOffsetRegistry>,
    _batch_ingestor: BatchIngestorClient,
    address: SocketAddr,
    ct: CancellationToken,
) -> CliResult<()> {
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            wings_metadata_core::protocol::admin_file_descriptor_set(),
        )
        .build_v1()
        .change_context(CliError::Service {
            message: "failed to create tonic reflection service".to_string(),
        })?;

    let admin_service = AdminService::new(admin).into_service();
    let offset_registry_service = OffsetRegistryService::new(offset_registry).into_service();

    let server = tonic::transport::Server::builder()
        .add_service(reflection_service)
        .add_service(admin_service)
        .add_service(offset_registry_service)
        .serve_with_shutdown(address, async move {
            ct.cancelled().await;
        });

    server.await.change_context(CliError::Server {
        message: "error while running grpc server".to_string(),
    })
}

async fn run_http_ingestor(
    admin: Arc<InMemoryAdminService>,
    batch_ingestor: BatchIngestorClient,
    address: SocketAddr,
    ct: CancellationToken,
) -> CliResult<()> {
    let topic_cache = TopicCache::new(admin.clone());
    let namespace_cache = NamespaceCache::new(admin);

    let app = {
        let ingestor = HttpIngestor::new(topic_cache, namespace_cache, batch_ingestor);
        let server = HttpServer::new();

        Router::new()
            .merge(ingestor.into_router())
            .merge(server.into_router())
    };

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .change_context(CliError::Server {
            message: format!("failed to bind address {address}"),
        })?;

    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        ct.cancelled().await;
    });

    server.await.change_context(CliError::Server {
        message: "server failed to run".to_string(),
    })
}
