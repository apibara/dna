//! Simple status server that always returns OK.

use std::net::SocketAddr;

use tokio_util::sync::CancellationToken;
use tracing::info;
use warp::Filter;

#[derive(Clone)]
pub struct StatusServer {
    address: SocketAddr,
}

impl StatusServer {
    pub fn new(address: SocketAddr) -> Self {
        StatusServer { address }
    }

    /// Starts the status server.
    pub async fn start(self, ct: CancellationToken) {
        let routes = warp::path("status").map(|| "OK");

        let (_, server) = warp::serve(routes)
            .try_bind_with_graceful_shutdown(self.address, async move { ct.cancelled().await })
            .expect("failed to bind status server");
        info!(address = ?self.address, "starting status server");
        server.await;
    }
}
