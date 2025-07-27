pub mod fetch;

use axum::{Router, routing::post};
use fetch::fetch_handler;

pub struct HttpServer {}

impl HttpServer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn into_router(self) -> Router {
        Router::new().route("/v1/fetch", post(fetch_handler))
    }
}
