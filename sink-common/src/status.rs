//! Simple status server that always returns OK.

use std::net::SocketAddr;

use tracing::info;
use warp::Filter;

/// Starts the status server at the provided address.
pub async fn start_status_server(status_address: SocketAddr) {
    let routes = warp::path("status").map(|| "OK");

    let server = warp::serve(routes).try_bind(status_address);

    info!("starting status server at {}", status_address);

    server.await
}
