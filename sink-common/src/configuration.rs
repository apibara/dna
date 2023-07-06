use std::{
    fs::File,
    io::BufReader,
    net::{AddrParseError, SocketAddr},
    path::Path,
};

use apibara_core::node::v1alpha2::DataFinality;
use apibara_sdk::{
    Configuration, InvalidMetadataKey, InvalidMetadataValue, InvalidUri, MetadataKey, MetadataMap,
    MetadataValue,
};
use apibara_transformer::{Transformer, TransformerError, TransformerOptions};
use bytesize::ByteSize;
use clap::Args;
use prost::Message;
use serde::de;

use crate::{connector::SinkConnectorOptions, persistence::Persistence};

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("Failed to build filter: {0}")]
    Filter(#[from] FilterError),
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("Failed to parse key: {0}")]
    ParseKey(#[from] InvalidMetadataKey),
    #[error("Failed to parse value: {0}")]
    ParseValue(#[from] InvalidMetadataValue),
    #[error("Invalid metadata format")]
    InvalidFormat,
}

#[derive(Debug, thiserror::Error)]
pub enum StatusServerError {
    #[error("Failed to parse status server address: {0}")]
    Address(#[from] AddrParseError),
}

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorFromConfigurationError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    StatusServer(#[from] StatusServerError),
    #[error("Failed to parse stream URL: {0}")]
    Uri(#[from] InvalidUri),
    #[error("Failed to parse transform: {0}")]
    Transform(#[from] TransformerError),
    #[error("Failed to message size: {0}")]
    SizeConversion(String),
}

/// Stream configuration command line flags.
#[derive(Args, Debug)]
pub struct ConfigurationArgs {
    /// Set the response preferred batch size.
    #[arg(long, env)]
    pub batch_size: Option<u64>,
    /// The json-encoded filter to use. If it starts with `@`, it is interpreted as a path to a file.
    #[arg(long, env)]
    pub filter: String,
    /// Path to a Javascript/Typescript transformation file to apply to data.
    #[arg(long, env)]
    pub transform: Option<String>,
    /// Load environment variables from this file.
    #[arg(long, env)]
    pub env_file: Option<String>,
    /// Limits the maximum size of a decoded message. Accept message size in human readable form,
    /// e.g. 1kb, 1MB, 1GB. If not set the default is 1MB.
    #[arg(long, env)]
    pub max_message_size: Option<String>,
    /// Add metadata to the stream, in the `key: value` format. Can be specified multiple times.
    #[arg(long, short = 'M', env)]
    pub metadata: Vec<String>,
    /// Use the authorization together when connecting to the stream.
    #[arg(long, short = 'A', env)]
    pub auth_token: Option<String>,
    /// DNA stream url. If starting with `https://`, use a secure connection.
    #[arg(long, env)]
    pub stream_url: String,
    #[command(flatten)]
    pub finality: Option<FinalityArgs>,
    #[command(flatten)]
    pub starting_cursor: StartingCursorArgs,
    #[command(flatten)]
    pub network: NetworkTypeArgs,
    #[command(flatten)]
    pub persistence: PersistenceArgs,
    #[command(flatten)]
    pub status_server: StatusServerArgs,
}

/// Stream configuration without finality
#[derive(Args, Debug)]
pub struct ConfigurationArgsWithoutFinality {
    /// Set the response preferred batch size.
    #[arg(long, env)]
    pub batch_size: Option<u64>,
    /// The json-encoded filter to use. If it starts with `@`, it is interpreted as a path to a file.
    #[arg(long, env)]
    pub filter: String,
    /// Jsonnet transformation to apply to data. If it starts with `@`, it is interpreted as a path to a file.
    #[arg(long, env)]
    pub transform: Option<String>,
    /// Load environment variables from this file.
    #[arg(long, env)]
    pub env_file: Option<String>,
    /// Limits the maximum size of a decoded message. Accept message size in human readable form,
    /// e.g. 1kb, 1MB, 1GB. If not set the default is 1MB.
    #[arg(long, env)]
    pub max_message_size: Option<String>,
    /// Add metadata to the stream, in the `key: value` format. Can be specified multiple times.
    #[arg(long, short = 'M', env)]
    pub metadata: Vec<String>,
    /// Use the authorization together when connecting to the stream.
    #[arg(long, short = 'A', env)]
    pub auth_token: Option<String>,
    /// DNA stream url. If starting with `https://`, use a secure connection.
    #[arg(long, env)]
    pub stream_url: String,
    #[command(flatten)]
    pub starting_cursor: StartingCursorArgs,
    #[command(flatten)]
    pub network: NetworkTypeArgs,
    #[command(flatten)]
    pub persistence: PersistenceArgs,
    #[command(flatten)]
    pub status_server: StatusServerArgs,
}

#[derive(Args, Debug)]
#[group(required = false, multiple = false)]
pub struct StartingCursorArgs {
    /// Start streaming data from this block.
    #[arg(long, env)]
    pub starting_block: Option<u64>,
}

#[derive(Args, Debug)]
#[group(required = true, multiple = false)]
pub struct NetworkTypeArgs {
    /// Stream Starknet network data.
    #[arg(long, env("NETWORK_STARKNET"))]
    pub starknet: bool,
}

/// Data finality flags.
#[derive(Args, Debug)]
#[group(required = false, multiple = false)]
pub struct FinalityArgs {
    /// Request finalized blocks.
    #[arg(long, env)]
    pub finalized: bool,
    /// Request accepted blocks.
    #[arg(long, env)]
    pub accepted: bool,
    /// Request pending blocks.
    #[arg(long, env)]
    pub pending: bool,
}

/// Flags related to state persistence.
#[derive(Args, Debug)]
pub struct PersistenceArgs {
    #[arg(long, env, requires = "sink_id")]
    /// URL to the etcd server used to persist data.
    pub persist_to_etcd: Option<String>,
    #[arg(long, env)]
    /// Unique identifier for this sink.
    pub sink_id: Option<String>,
}

/// Flags related to the status server.
#[derive(Args, Debug)]
pub struct StatusServerArgs {
    /// Address to bind the status server to.
    #[arg(long, env, default_value = "0.0.0.0:8118")]
    pub status_server_address: String,
}

impl ConfigurationArgs {
    pub fn to_sink_connector_options<F>(
        &self,
    ) -> Result<SinkConnectorOptions<F>, SinkConnectorFromConfigurationError>
    where
        F: Message + Default + Clone + de::DeserializeOwned,
    {
        let max_message_size: ByteSize = self
            .max_message_size
            .as_ref()
            .map(|s| s.parse())
            .transpose()
            .map_err(SinkConnectorFromConfigurationError::SizeConversion)?
            .unwrap_or(ByteSize::mb(1));
        let configuration = self.to_configuration::<F>()?;
        let stream_url = self.stream_url.parse()?;
        let transformer = self.to_transformer()?;
        let persistence = self.to_persistence();
        let metadata = self.to_metadata()?;
        let bearer_token = self.to_bearer_token();
        let status_server_address = self.to_status_server_address()?;
        Ok(SinkConnectorOptions {
            stream_url,
            configuration,
            transformer,
            metadata,
            bearer_token,
            persistence,
            max_message_size: max_message_size.as_u64() as usize,
            status_server_address,
        })
    }

    pub fn to_configuration<F>(&self) -> Result<Configuration<F>, ConfigurationError>
    where
        F: Message + Default + Clone + de::DeserializeOwned,
    {
        let filter: F = self.new_filter()?;
        let mut configuration = Configuration::<F>::default().with_filter(|_| filter.clone());
        configuration = if let Some(finality) = &self.finality {
            let finality = finality.to_finality();
            configuration.with_finality(finality)
        } else {
            configuration
        };

        configuration = if let Some(batch_size) = self.batch_size {
            configuration.with_batch_size(batch_size)
        } else {
            configuration
        };

        configuration = if let Some(starting_block) = self.starting_cursor.starting_block {
            configuration.with_starting_block(starting_block)
        } else {
            configuration
        };

        Ok(configuration)
    }

    /// Returns the Deno transformation to apply to data.
    pub fn to_transformer(&self) -> Result<Option<Transformer>, TransformerError> {
        // only forward variables from env file to transformer.
        let allow_env = if let Some(env_file) = &self.env_file {
            let mut variables = Vec::default();
            for item in dotenvy::from_path_iter(env_file)? {
                let (key, value) = item?;
                std::env::set_var(&key, value);
                variables.push(key);
            }
            Some(variables)
        } else {
            None
        };

        if let Some(transform) = &self.transform {
            let current_dir = std::env::current_dir().expect("current directory");
            let transformer_options = TransformerOptions { allow_env };
            let transformer = Transformer::from_file(transform, current_dir, transformer_options)?;
            Ok(Some(transformer))
        } else {
            Ok(None)
        }
    }

    pub fn to_persistence(&self) -> Option<Persistence> {
        if let Some(etcd_url) = &self.persistence.persist_to_etcd {
            let sink_id = self
                .persistence
                .sink_id
                .as_ref()
                .expect("sink_id is required when persist_to_etcd is set");
            let persistence = Persistence::new(etcd_url, sink_id);
            Some(persistence)
        } else {
            None
        }
    }

    pub fn to_metadata(&self) -> Result<MetadataMap, MetadataError> {
        let mut metadata = MetadataMap::new();
        for entry in &self.metadata {
            match entry.split_once(':') {
                None => return Err(MetadataError::InvalidFormat),
                Some((key, value)) => {
                    let key: MetadataKey = key.parse()?;
                    let value: MetadataValue = value.parse()?;
                    metadata.insert(key, value);
                }
            }
        }

        Ok(metadata)
    }

    pub fn to_bearer_token(&self) -> Option<String> {
        self.auth_token.clone()
    }

    pub fn to_status_server_address(&self) -> Result<SocketAddr, StatusServerError> {
        let address: SocketAddr = self.status_server.status_server_address.parse()?;
        Ok(address)
    }

    fn new_filter<F>(&self) -> Result<F, FilterError>
    where
        F: Message + Default + Clone + de::DeserializeOwned,
    {
        if self.filter.starts_with('@') {
            let filter_path = Path::new(&self.filter[1..]);
            let filter_file = File::open(filter_path)?;
            let filter_reader = BufReader::new(filter_file);
            let filter = serde_json::from_reader(filter_reader)?;
            Ok(filter)
        } else {
            let filter = serde_json::from_str(&self.filter)?;
            Ok(filter)
        }
    }
}

impl FinalityArgs {
    pub fn to_finality(&self) -> DataFinality {
        if self.pending {
            DataFinality::DataStatusPending
        } else if self.accepted {
            DataFinality::DataStatusAccepted
        } else if self.finalized {
            DataFinality::DataStatusFinalized
        } else {
            DataFinality::DataStatusAccepted
        }
    }
}

impl From<ConfigurationArgsWithoutFinality> for ConfigurationArgs {
    fn from(value: ConfigurationArgsWithoutFinality) -> Self {
        Self {
            batch_size: value.batch_size,
            filter: value.filter,
            transform: value.transform,
            max_message_size: value.max_message_size,
            metadata: value.metadata,
            auth_token: value.auth_token,
            stream_url: value.stream_url,
            env_file: value.env_file,
            finality: Some(FinalityArgs {
                finalized: true,
                accepted: false,
                pending: false,
            }),
            starting_cursor: value.starting_cursor,
            network: value.network,
            persistence: value.persistence,
            status_server: value.status_server,
        }
    }
}
