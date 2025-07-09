//! HTTP ingestor server.
//!
//! This crate provides a server to ingest messages over HTTP.
//!
//! The server is built using axum and provides
//! a `/v1/push` endpoint for message ingestion.
//!
//! # Example
//!
//! ```rust
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//! use tokio_util::sync::CancellationToken;
//! use wings_ingestor_http::HttpIngestor;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
//! let ingestor = HttpIngestor::new(addr);
//! let ct = CancellationToken::new();
//!
//! // This would run the server until cancellation
//! // ingestor.run(ct).await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod push;

// Re-export the main types for easier importing
pub use error::{HttpIngestorError, HttpIngestorResult};

use std::net::SocketAddr;

use axum::{Router, routing::post};
use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;

use crate::push::push_handler;

/// HTTP ingestor server that receives messages via HTTP POST requests.
pub struct HttpIngestor {
    listen_addr: SocketAddr,
}

impl HttpIngestor {
    /// Create a new HTTP ingestor with the specified listen address.
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self { listen_addr }
    }

    /// Run the HTTP ingestor server.
    ///
    /// This method starts the axum server and listens for incoming requests.
    /// The server will gracefully shutdown when the cancellation token is cancelled.
    pub async fn run(self, ct: CancellationToken) -> HttpIngestorResult<()> {
        let app = Router::new().route("/v1/push", post(push_handler));

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
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let ingestor = HttpIngestor::new(addr);
        assert_eq!(ingestor.listen_addr, addr);
    }

    #[tokio::test]
    async fn test_push_endpoint_returns_ok() {
        let app = Router::new().route("/v1/push", post(push_handler));

        let request = Request::builder()
            .method("POST")
            .uri("/v1/push")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("\"status\":\"ok\""));
    }
}
