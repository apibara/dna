use std::net::SocketAddr;

use clap::Args;
use error_stack::{Result, ResultExt};

use crate::server::ServerOptions;

use super::{error::ServerError, StreamServiceOptions};

#[derive(Args, Debug)]
pub struct ServerArgs {
    /// Whether to run the DNA server.
    #[clap(long = "server.enabled", env = "DNA_SERVER_ENABLED")]
    pub server_enabled: bool,
    /// The DNA server address.
    #[clap(
        long = "server.address",
        env = "DNA_SERVER_ADDRESS",
        default_value = "0.0.0.0:7007"
    )]
    pub server_address: String,
    /// Maximum number of concurrent streams served.
    #[clap(
        long = "server.max-concurrent-streams",
        env = "DNA_SERVER_MAX_CONCURRENT_STREAMS",
        default_value = "1000"
    )]
    pub server_max_concurrent_streams: usize,
    /// Number of prefetch segments.
    #[clap(
        long = "server.prefetch-segment-count",
        env = "DNA_SERVER_PREFETCH_SEGMENT_COUNT",
        default_value = "128"
    )]
    pub server_prefetch_segment_count: usize,
    /// Size of the channel used to send data to the stream.
    #[clap(
        long = "server.channel-size",
        env = "DNA_SERVER_CHANNEL_SIZE",
        default_value = "128"
    )]
    pub server_channel_size: usize,
}

impl ServerArgs {
    pub fn to_server_options(&self) -> Result<ServerOptions, ServerError> {
        let address = self
            .server_address
            .parse::<SocketAddr>()
            .change_context(ServerError)
            .attach_printable("failed to parse server address")
            .attach_printable_lazy(|| format!("address: {}", self.server_address))?;

        let stream_service_options = StreamServiceOptions {
            max_concurrent_streams: self.server_max_concurrent_streams,
            prefetch_segment_count: self.server_prefetch_segment_count,
            channel_size: self.server_channel_size,
        };

        Ok(ServerOptions {
            address,
            stream_service_options,
        })
    }
}
