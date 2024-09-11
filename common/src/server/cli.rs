use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use clap::Args;
use error_stack::{Result, ResultExt};

use crate::{file_cache::FileCacheOptions, server::ServerOptions};

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
    /// Where to store cached data.
    #[clap(long = "server.cache-dir", env = "DNA_SERVER_CACHE_DIR")]
    pub server_cache_dir: Option<String>,
    /// Maximum size of the cache.
    #[clap(
        long = "server.cache-size",
        env = "DNA_SERVER_CACHE_SIZE",
        default_value = "10Gi"
    )]
    pub server_cache_size: String,
    /// Maximum number of concurrent streams served.
    #[clap(
        long = "server.max-concurrent-streams",
        env = "DNA_SERVER_MAX_CONCURRENT_STREAMS",
        default_value = "1000"
    )]
    pub max_concurrent_streams: usize,
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
            max_concurrent_streams: self.max_concurrent_streams,
        };

        Ok(ServerOptions {
            address,
            stream_service_options,
        })
    }

    pub fn to_file_cache_options(&self) -> Result<FileCacheOptions, ServerError> {
        let cache_dir = if let Some(cache_dir) = &self.server_cache_dir {
            cache_dir
                .parse::<PathBuf>()
                .change_context(ServerError)
                .attach_printable("failed to parse cache dir")
                .attach_printable_lazy(|| format!("cache dir: {}", cache_dir))?
        } else {
            dirs::data_local_dir()
                .ok_or(ServerError)
                .attach_printable("failed to get data dir")?
                .join("dna")
        };

        let max_size_bytes = byte_unit::Byte::from_str(&self.server_cache_size)
            .change_context(ServerError)
            .attach_printable("failed to parse cache size")
            .attach_printable_lazy(|| format!("cache size: {}", self.server_cache_size))?
            .as_u64();

        Ok(FileCacheOptions {
            base_dir: cache_dir,
            max_size_bytes,
        })
    }
}
