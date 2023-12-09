use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Configuration {
    /// The gRPC server address.
    pub address: SocketAddr,
}
