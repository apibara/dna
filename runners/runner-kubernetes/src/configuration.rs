use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct Configuration {
    /// The gRPC server address.
    pub address: SocketAddr,
    /// The namespace where to create resources.
    pub target_namespace: String,
}
