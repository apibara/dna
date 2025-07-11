//! HTTP ingestor server.
//!
//! This crate provides a server to ingest messages over HTTP.
//!
//! The server is built using axum and provides
//! a `/v1/push` endpoint for message ingestion.

pub mod error;
pub mod push;
pub mod types;

// Re-export the main types for easier importing
pub use error::{HttpIngestorError, HttpIngestorResult};
pub use types::{Batch, PushRequest, PushResponse};

use std::net::SocketAddr;

use axum::{Router, routing::post};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::cache::TopicCache;

use crate::push::push_handler;

/// HTTP ingestor server that receives messages via HTTP POST requests.
pub struct HttpIngestor {
    listen_addr: SocketAddr,
    topic_cache: TopicCache,
}

#[derive(Clone)]
pub struct HttpIngestorState {
    topic_cache: TopicCache,
}

impl HttpIngestor {
    /// Create a new HTTP ingestor with the specified listen address and topic cache.
    pub fn new(listen_addr: SocketAddr, topic_cache: TopicCache) -> Self {
        Self {
            listen_addr,
            topic_cache,
        }
    }

    /// Run the HTTP ingestor server.
    ///
    /// This method starts the axum server and listens for incoming requests.
    /// The server will gracefully shutdown when the cancellation token is cancelled.
    pub async fn run(self, ct: CancellationToken) -> HttpIngestorResult<()> {
        let app = Router::new()
            .route("/v1/push", post(push_handler))
            .with_state(HttpIngestorState::new(self.topic_cache));

        let listener = tokio::net::TcpListener::bind(&self.listen_addr)
            .await
            .change_context(HttpIngestorError::BindError {
                address: self.listen_addr.to_string(),
            })?;

        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            ct.cancelled().await;
        });

        server.await.change_context(HttpIngestorError::ServerError {
            message: "server failed to run".to_string(),
        })
    }
}

impl HttpIngestorState {
    pub fn new(topic_cache: TopicCache) -> Self {
        Self { topic_cache }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use std::net::{IpAddr, Ipv4Addr};
    use tower::ServiceExt;

    #[test]
    fn test_http_ingestor_creation() {
        use std::sync::Arc;
        use wings_metadata_core::admin::memory::InMemoryAdminService;
        use wings_metadata_core::cache::TopicCache;

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let admin = Arc::new(InMemoryAdminService::new());
        let topic_cache = TopicCache::new(admin);
        let ingestor = HttpIngestor::new(addr, topic_cache);
        assert_eq!(ingestor.listen_addr, addr);
    }

    #[tokio::test]
    async fn test_push_endpoint_returns_ok() {
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;
        use wings_metadata_core::admin::memory::InMemoryAdminService;
        use wings_metadata_core::admin::{
            Admin, NamespaceName, NamespaceOptions, SecretName, TenantName, TopicName, TopicOptions,
        };
        use wings_metadata_core::cache::TopicCache;

        let admin = Arc::new(InMemoryAdminService::new());

        // Create test data in metadata
        let tenant_name = TenantName::new("test-tenant").unwrap();
        let namespace_name = NamespaceName::new("test-namespace", tenant_name.clone()).unwrap();
        let topic_name = TopicName::new("test-topic", namespace_name.clone()).unwrap();

        // Create tenant, namespace, and topic
        admin.create_tenant(tenant_name).await.unwrap();
        admin
            .create_namespace(
                namespace_name.clone(),
                NamespaceOptions::new(SecretName::new("test-config").unwrap()),
            )
            .await
            .unwrap();

        let topic_options = TopicOptions::new(vec![Field::new("field", DataType::Utf8, false)]);
        admin.create_topic(topic_name, topic_options).await.unwrap();

        let topic_cache = TopicCache::new(admin);
        let app = Router::new()
            .route("/v1/push", post(push_handler))
            .with_state(HttpIngestorState::new(topic_cache));

        let push_request = PushRequest {
            namespace: "tenants/test-tenant/namespaces/test-namespace".to_string(),
            batches: vec![Batch {
                topic: "test-topic".to_string(),
                partition: None,
                data: vec![serde_json::json!({"field": "value"})],
            }],
        };

        let request = Request::builder()
            .method("POST")
            .uri("/v1/push")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&push_request).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        let response_obj: PushResponse = serde_json::from_str(&body_str).unwrap();
        assert_eq!(response_obj, PushResponse::new());
    }
}
