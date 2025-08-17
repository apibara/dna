use std::{sync::Arc, time::Duration};

use apibara_dna_protocol::dna::stream::dna_stream_client::DnaStreamClient;
use snafu::{ResultExt, Snafu};
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, ClientTlsConfig, Uri},
};

use crate::{auth::BearerTokenProvider, client::StreamClient, interceptor::MetadataInterceptor};

/// Configure the [StreamClient] before connecting to the server.
pub struct StreamClientBuilder {
    token_provider: Option<Arc<dyn BearerTokenProvider>>,
    max_message_size: Option<usize>,
    metadata: MetadataMap,
    timeout: Duration,
    tls_config: Option<ClientTlsConfig>,
}

/// The error type for the [StreamClientBuilder].
#[derive(Debug, Snafu)]
pub enum StreamClientBuilderError {
    /// Connection error.
    #[snafu(display("Failed to connect to the server"))]
    Connection { source: tonic::transport::Error },
    /// Error caused by the [BearerTokenProvider].
    #[snafu(display("Failed to get bearer token"))]
    BearerToken {
        source: crate::auth::BearerTokenError,
    },
}

impl StreamClientBuilder {
    /// Create a new [StreamClientBuilder].
    pub fn new() -> Self {
        Default::default()
    }

    /// Use the given [BearerTokenProvider] to authenticate with the server.
    pub fn with_bearer_token_provider(mut self, provider: Arc<dyn BearerTokenProvider>) -> Self {
        self.token_provider = Some(provider);
        self
    }

    /// Use the given `metadata` when connecting to the server.
    ///
    /// Notice: metadata will be merged with the authentication header if any.
    pub fn with_metadata(mut self, metadata: MetadataMap) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set the maximum time to wait for a message from the server.
    ///
    /// Internally, the client requests a heartbeat message every `timeout / 2`.
    /// Notice that depending on the server configuration, the request may fail
    /// if the requested timeout is too short or too long.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum message size (in bytes) that the client can receive.
    pub fn with_max_message_size_bytes(mut self, message_size: usize) -> Self {
        self.max_message_size = Some(message_size);
        self
    }

    /// Connect to the server, returning a [StreamClient] ready to use.
    pub async fn connect(self, url: Uri) -> Result<StreamClient, StreamClientBuilderError> {
        let tls = self
            .tls_config
            .unwrap_or_else(|| ClientTlsConfig::default().with_native_roots());

        let channel = Channel::builder(url)
            .tls_config(tls)
            .context(ConnectionSnafu {})?
            .connect()
            .await
            .context(ConnectionSnafu {})?;

        let mut interceptor = MetadataInterceptor::with_metadata(self.metadata);
        if let Some(token_provider) = self.token_provider {
            let token = token_provider.get_token().context(BearerTokenSnafu {})?;
            interceptor
                .insert_bearer_token(token)
                .map_err(Into::into)
                .context(BearerTokenSnafu {})?;
        }

        let mut default_client = DnaStreamClient::with_interceptor(channel, interceptor);
        default_client = if let Some(max_message_size) = self.max_message_size {
            default_client.max_decoding_message_size(max_message_size)
        } else {
            default_client
        };

        Ok(StreamClient::new(default_client, self.timeout))
    }
}

impl Default for StreamClientBuilder {
    fn default() -> Self {
        Self {
            token_provider: None,
            max_message_size: None,
            metadata: MetadataMap::new(),
            timeout: Duration::from_secs(60),
            tls_config: None,
        }
    }
}
