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
use wings_ingestor_core::BatchIngestorClient;

use std::net::SocketAddr;

use axum::{Router, routing::post};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::cache::{NamespaceCache, TopicCache};

use crate::push::push_handler;

/// HTTP ingestor server that receives messages via HTTP POST requests.
pub struct HttpIngestor {
    listen_addr: SocketAddr,
    state: HttpIngestorState,
}

#[derive(Clone)]
pub struct HttpIngestorState {
    topic_cache: TopicCache,
    namespace_cache: NamespaceCache,
    batch_ingestion: BatchIngestorClient,
}

impl HttpIngestor {
    /// Create a new HTTP ingestor with the specified listen address and topic cache.
    pub fn new(
        listen_addr: SocketAddr,
        topic_cache: TopicCache,
        namespace_cache: NamespaceCache,
        batch_ingestion: BatchIngestorClient,
    ) -> Self {
        let state = HttpIngestorState {
            topic_cache,
            namespace_cache,
            batch_ingestion,
        };

        Self { listen_addr, state }
    }

    /// Run the HTTP ingestor server.
    ///
    /// This method starts the axum server and listens for incoming requests.
    /// The server will gracefully shutdown when the cancellation token is cancelled.
    pub async fn run(self, ct: CancellationToken) -> HttpIngestorResult<()> {
        let app = Router::new()
            .route("/v1/push", post(push_handler))
            .with_state(self.state);

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
