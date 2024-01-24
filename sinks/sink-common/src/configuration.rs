use std::{env, fmt, net::AddrParseError, path::PathBuf, str::FromStr, time::Duration};

use apibara_core::{node::v1alpha2::DataFinality, starknet::v1alpha2};
use apibara_script::ScriptOptions as IndexerOptions;
use apibara_sdk::{Configuration, MetadataKey, MetadataMap, MetadataValue, Uri};
use bytesize::ByteSize;
use clap::Args;
use error_stack::{Result, ResultExt};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{connector::StreamConfiguration, status::StatusServer};

#[derive(Debug, Deserialize)]
pub struct OptionsFromScript {
    #[serde(flatten)]
    pub stream: StreamOptions,
    #[serde(flatten)]
    pub stream_configuration: StreamConfigurationOptions,
}

#[derive(Args, Debug)]
pub struct OptionsFromCli {
    #[clap(flatten)]
    pub connector: ConnectorOptions,
    #[clap(flatten)]
    pub stream: StreamOptions,
}

/// Options for the connector persistence.
#[derive(Args, Debug, Default, Deserialize)]
pub struct PersistenceOptions {
    #[command(flatten)]
    pub persistence_type: PersistenceTypeOptions,
    #[arg(long, env)]
    /// Unique identifier for this sink.
    pub sink_id: Option<String>,
}

#[derive(Args, Debug, Default, Deserialize)]
#[group(required = false, multiple = false)]
pub struct PersistenceTypeOptions {
    #[arg(long, env, requires = "sink_id")]
    /// URL to the etcd server used to persist data.
    pub persist_to_etcd: Option<String>,
    #[arg(long, env, requires = "sink_id")]
    /// Path to the directory used to persist data.
    pub persist_to_fs: Option<String>,
    #[arg(long, env, requires = "sink_id")]
    /// URL to the redis server used to persist data.
    pub persist_to_redis: Option<String>,
}

/// Status server options.
#[derive(Args, Debug, Default)]
pub struct StatusServerOptions {
    /// Address to bind the status server to.
    #[arg(long, env)]
    pub status_server_address: Option<String>,
}

#[derive(Args, Debug, Default)]
pub struct ConnectorOptions {
    #[command(flatten)]
    pub persistence: PersistenceOptions,
    #[command(flatten)]
    pub status_server: StatusServerOptions,
    #[command(flatten)]
    pub script: ScriptOptions,
}

#[derive(Args, Debug, Default, Clone)]
pub struct ScriptOptions {
    /// Load script environment variables from the specified file.
    ///
    /// Notice that by default the script doesn't have access to any environment variable,
    /// only from the ones specified in this file.
    #[arg(long, env)]
    pub allow_env: Option<PathBuf>,
    /// Grant access to the specified environment variables.
    #[arg(long, env, value_delimiter = ',')]
    pub allow_env_from_env: Option<Vec<String>>,
    /// Grant network access to the hosts.
    ///
    /// Leave empty to allow all hosts, i.e. by specifying `--allow-net`.
    #[arg(long, env, value_delimiter = ',', num_args = 0..)]
    pub allow_net: Option<Vec<String>>,
    /// Maximum time allowed to execute the transform function.
    #[arg(long, env)]
    pub script_transform_timeout_seconds: Option<u64>,
    /// Maximum time allowed to load the indexer script.
    #[arg(long, env)]
    pub script_load_timeout_seconds: Option<u64>,
}

#[derive(Args, Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamOptions {
    /// DNA stream url. If starting with `https://`, use a secure connection.
    #[arg(long, env)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_url: Option<String>,
    /// Limits the maximum size of a decoded message. Accept message size in human readable form,
    /// e.g. 1kb, 1MB, 1GB. If not set the default is 1MB.
    #[arg(long, env)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_message_size: Option<String>,
    /// Add metadata to the stream, in the `key: value` format. Can be specified multiple times.
    #[arg(long, short = 'M', env, value_delimiter = ',')]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
    /// Use the authorization together when connecting to the stream.
    #[arg(long, short = 'A', env)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    /// Maximum timeout (in seconds) between stream messages. Defaults to 45s.
    #[arg(long, env)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_duration_seconds: Option<u64>,
    /// Stop streaming data at (before) the specified block (non inclusive).
    ///
    /// This option can be used to quickly backfill missing data. Notice that you must ensure that
    /// `ending_block` has been finalized or you may end up with duplicate data.
    ///
    /// The `ending_block` is non inclusive so that you can use the same value as another's indexer
    /// `starting_block` to avoid duplicate data.
    ///
    /// The `ending_block` must be greater than the `starting_block`.
    ///
    /// If not specified, the stream will continue indefinitely.
    #[arg(long, env)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_block: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StreamConfigurationOptions {
    /// The data filter.
    #[serde(flatten)]
    pub filter: NetworkFilterOptions,
    /// Set the response preferred batch size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u64>,
    /// The finality of the data to be streamed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finality: Option<DataFinality>,
    /// Start streaming data from the specified block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub starting_block: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "network", content = "filter", rename_all = "camelCase")]
pub enum NetworkFilterOptions {
    Starknet(v1alpha2::Filter),
}

impl StatusServerOptions {
    pub fn to_status_server(self) -> Result<StatusServer, AddrParseError> {
        let address = self
            .status_server_address
            .unwrap_or_else(|| "0.0.0.0:0".to_string())
            .parse()?;
        Ok(StatusServer::new(address))
    }
}

#[derive(Debug)]
pub struct MissingStreamUrlError;
impl error_stack::Context for MissingStreamUrlError {}

impl fmt::Display for MissingStreamUrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("missing stream url")
    }
}

#[derive(Debug)]
pub struct StreamOptionsError;
impl error_stack::Context for StreamOptionsError {}

impl fmt::Display for StreamOptionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid stream options")
    }
}

#[derive(Debug)]
pub struct InvalidByteSizeError(String);
impl error_stack::Context for InvalidByteSizeError {}

impl fmt::Display for InvalidByteSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid byte size: {}", self.0)
    }
}

impl StreamOptions {
    pub fn merge(self, other: StreamOptions) -> StreamOptions {
        StreamOptions {
            stream_url: self.stream_url.or(other.stream_url),
            max_message_size: self.max_message_size.or(other.max_message_size),
            metadata: self.metadata.or(other.metadata),
            auth_token: self.auth_token.or(other.auth_token),
            timeout_duration_seconds: self
                .timeout_duration_seconds
                .or(other.timeout_duration_seconds),
            ending_block: self.ending_block.or(other.ending_block),
        }
    }

    pub fn to_stream_configuration(self) -> Result<StreamConfiguration, StreamOptionsError> {
        let stream_url: Uri = self
            .stream_url
            .ok_or(MissingStreamUrlError)
            .change_context(StreamOptionsError)?
            .parse::<Uri>()
            .change_context(StreamOptionsError)?;
        let max_message_size_bytes: ByteSize = self
            .max_message_size
            .as_ref()
            .map(|s| ByteSize::from_str(s))
            .transpose()
            .map_err(InvalidByteSizeError)
            .change_context(StreamOptionsError)?
            .unwrap_or(ByteSize::mb(100));

        let timeout_duration = Duration::from_secs(self.timeout_duration_seconds.unwrap_or(45));

        let mut metadata = MetadataMap::new();
        for entry in self.metadata.unwrap_or_default() {
            match entry.split_once(':') {
                None => {
                    return Err(StreamOptionsError)
                        .attach_printable("metadata must be in the `key: value format")
                        .attach_printable_lazy(|| format!("got: {entry}"))
                }
                Some((key, value)) => {
                    let key = key
                        .parse::<MetadataKey>()
                        .change_context(StreamOptionsError)
                        .attach_printable_lazy(|| format!("invalid metadata key: {key}"))?;
                    let value = value
                        .parse::<MetadataValue>()
                        .change_context(StreamOptionsError)
                        .attach_printable_lazy(|| format!("invalid metadata value: {value}"))?;
                    metadata.insert(key, value);
                }
            }
        }

        Ok(StreamConfiguration {
            stream_url,
            max_message_size_bytes,
            metadata,
            bearer_token: self.auth_token,
            timeout_duration,
            ending_block: self.ending_block,
        })
    }
}

impl StreamConfigurationOptions {
    pub fn merge(self, other: StreamConfigurationOptions) -> StreamConfigurationOptions {
        StreamConfigurationOptions {
            filter: self.filter,
            batch_size: self.batch_size.or(other.batch_size),
            finality: self.finality.or(other.finality),
            starting_block: self.starting_block.or(other.starting_block),
        }
    }

    /// Returns a `Configuration` object to stream Starknet data.
    pub fn as_starknet(&self) -> Option<Configuration<v1alpha2::Filter>> {
        let mut configuration = Configuration::default();

        configuration = if let Some(batch_size) = self.batch_size {
            configuration.with_batch_size(batch_size)
        } else {
            configuration
        };

        configuration = if let Some(finality) = self.finality {
            configuration.with_finality(finality)
        } else {
            configuration
        };

        // The starting block is inclusive, but the stream expects the index of the block
        // immediately before the first one sent.
        configuration = match self.starting_block {
            Some(starting_block) if starting_block > 0 => {
                configuration.with_starting_block(starting_block - 1)
            }
            _ => configuration,
        };

        match self.filter {
            NetworkFilterOptions::Starknet(ref filter) => {
                Some(configuration.with_filter(|_| filter.clone()))
            }
        }
    }
}

#[derive(Debug)]
pub struct ScriptOptionsError;
impl error_stack::Context for ScriptOptionsError {}

impl fmt::Display for ScriptOptionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid script options")
    }
}

impl ScriptOptions {
    /// Load environment variables from the specified dotenv file.
    ///
    /// Returns a copy of script options that inherits the environment variables from the
    /// environment.
    pub fn load_environment_variables(&self) -> Result<ScriptOptions, ScriptOptionsError> {
        let mut allow_env = vec![];
        if let Some(allow_env_file) = self.allow_env.as_ref() {
            let env_iter = dotenvy::from_path_iter(allow_env_file)
                .change_context(ScriptOptionsError)
                .attach_printable_lazy(|| {
                    format!(
                        "failed to load environment variables from path: {:?}",
                        allow_env
                    )
                })?;

            for item in env_iter {
                let (key, value) = item
                    .change_context(ScriptOptionsError)
                    .attach_printable("invalid environment variable")?;
                allow_env.push(key.clone());
                debug!(env = ?key, "allowing environment variable");
                env::set_var(key, value);
            }
        };

        if let Some(env_from_env) = self.allow_env_from_env.as_ref() {
            for key in env_from_env {
                allow_env.push(key.clone());
                debug!(env = ?key, "allowing environment variable");
            }
        }

        let allow_env_from_env = if allow_env.is_empty() {
            None
        } else {
            Some(allow_env)
        };

        Ok(ScriptOptions {
            allow_env: None,
            allow_env_from_env,
            ..self.clone()
        })
    }

    pub fn into_indexer_options(self) -> IndexerOptions {
        IndexerOptions {
            allow_env: self.allow_env_from_env,
            allow_net: self.allow_net,
            transform_timeout: self
                .script_transform_timeout_seconds
                .map(Duration::from_secs),
            load_timeout: self.script_load_timeout_seconds.map(Duration::from_secs),
        }
    }
}

#[cfg(test)]
mod tests {
    use apibara_core::node::v1alpha2::DataFinality;
    use bytesize::ByteSize;

    use super::{
        StatusServerOptions, StreamConfigurationOptions, StreamOptions, StreamOptionsError,
    };

    #[test]
    pub fn test_status_server_options() {
        let options = StatusServerOptions {
            status_server_address: Some("0.0.0.0:1111".to_string()),
        };
        let _ = options
            .to_status_server()
            .expect("convert to status server");
    }

    #[test]
    pub fn test_stream_options_from_json() {
        let json = r#"
        {
            "streamUrl": "https://test.test.a5a.ch",
            "maxMessageSize": "1MB",
            "metadata": ["key1: value1", "key2: value2"],
            "authToken": "auth_token"
        }
        "#;
        let config = serde_json::from_str::<StreamOptions>(json)
            .expect("parse StreamOptions from json")
            .to_stream_configuration()
            .expect("stream configuration");

        assert!(config.metadata.get("key2").is_some());
        assert_eq!(config.stream_url.scheme().unwrap(), "https");
    }

    #[test]
    pub fn test_stream_options_invalid_url() {
        let json = r#"
        {
            "streamUrl": "",
            "maxMessageSize": "1MB",
            "metadata": ["key1: value1", "key2: value2"],
            "authToken": "auth_token"
        }
        "#;
        let Err(err) = serde_json::from_str::<StreamOptions>(json)
            .expect("parse StreamOptions from json")
            .to_stream_configuration()
        else {
            panic!("expected error");
        };
        assert!(err.downcast_ref::<StreamOptionsError>().is_some());
    }

    #[test]
    pub fn test_stream_options_invalid_message_size() {
        let json = r#"
        {
            "streamUrl": "https://test.test.a5a.ch",
            "maxMessageSize": "xxxx",
            "metadata": ["key1: value1", "key2: value2"],
            "authToken": "auth_token"
        }
        "#;
        let Err(err) = serde_json::from_str::<StreamOptions>(json)
            .expect("parse StreamOptions from json")
            .to_stream_configuration()
        else {
            panic!("expected error");
        };
        assert!(err.downcast_ref::<StreamOptionsError>().is_some());
    }

    #[test]
    pub fn test_stream_options_invalid_metadata() {
        let json = r#"
        {
            "streamUrl": "https://test.test.a5a.ch",
            "maxMessageSize": "1MB",
            "metadata": ["key1 value1", "key2: value2"],
            "authToken": "auth_token"
        }
        "#;
        let Err(err) = serde_json::from_str::<StreamOptions>(json)
            .expect("parse StreamOptions from json")
            .to_stream_configuration()
        else {
            panic!("expected error");
        };
        assert!(err.downcast_ref::<StreamOptionsError>().is_some());
    }

    #[test]
    pub fn test_stream_options_invalid_metadata_key() {
        let json = r#"
        {
            "streamUrl": "https://test.test.a5a.ch",
            "maxMessageSize": "1MB",
            "metadata": ["key1 key1: value1", "key2: value2"],
            "authToken": "auth_token"
        }
        "#;
        let Err(err) = serde_json::from_str::<StreamOptions>(json)
            .expect("parse StreamOptions from json")
            .to_stream_configuration()
        else {
            panic!("expected error");
        };
        assert!(err.downcast_ref::<StreamOptionsError>().is_some());
    }

    #[test]
    pub fn test_stream_options_merge() {
        let json1 = r#"
        {
            "streamUrl": "https://test.test.a5a.ch",
            "maxMessageSize": "1MB",
            "metadata": ["key1 key1: value1", "key2: value2"]
        }
        "#;
        let config1 =
            serde_json::from_str::<StreamOptions>(json1).expect("parse StreamOptions from json");

        let json2 = r#"
        {
            "streamUrl": "https://secret.secret.a5a.ch",
            "metadata": []
        }
        "#;
        let config2 =
            serde_json::from_str::<StreamOptions>(json2).expect("parse StreamOptions from json");

        let config = config2
            .merge(config1)
            .to_stream_configuration()
            .expect("stream configuration");

        assert_eq!(
            config.stream_url.to_string(),
            "https://secret.secret.a5a.ch/"
        );
        assert_eq!(config.max_message_size_bytes, ByteSize::mb(1));
        assert!(config.metadata.is_empty());
        assert!(config.bearer_token.is_none());
    }

    #[test]
    pub fn test_stream_configuration_with_all_options() {
        let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "batchSize": 100,
            "finality": "DATA_STATUS_PENDING",
            "startingBlock": 0
        }
        "#;
        let config = serde_json::from_str::<StreamConfigurationOptions>(json)
            .expect("parse StreamConfigurationOptions from json")
            .as_starknet()
            .expect("starknet configuration");

        assert_eq!(config.batch_size, 100);
        assert_eq!(config.finality, Some(DataFinality::DataStatusPending));
        assert_eq!(config.starting_cursor, None);
        assert!(config.filter.header.is_some());
    }

    #[test]
    pub fn test_stream_configuration_with_only_filter() {
        let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            }
        }
        "#;
        let config = serde_json::from_str::<StreamConfigurationOptions>(json)
            .expect("parse StreamConfigurationOptions from json")
            .as_starknet()
            .expect("starknet configuration");

        assert_eq!(config.batch_size, 1);
        assert_eq!(config.finality, None);
        assert_eq!(config.starting_cursor, None);
        assert!(config.filter.header.is_some());
    }

    #[test]
    pub fn test_stream_configuration_adjusts_starting_block() {
        let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "startingBlock": 1000
        }
        "#;
        let config = serde_json::from_str::<StreamConfigurationOptions>(json)
            .expect("parse StreamConfigurationOptions from json")
            .as_starknet()
            .expect("starknet configuration");

        assert_eq!(config.starting_cursor.unwrap().order_key, 999);
    }

    #[test]
    pub fn test_stream_configuration_requires_filter() {
        let json = r#"
        {
            "network": "starknet"
        }
        "#;
        let config = serde_json::from_str::<StreamConfigurationOptions>(json);
        assert!(config.is_err());
    }

    #[test]
    pub fn test_stream_configuration_ignores_extra_fields() {
        let json = r#"
        {
            "network": "starknet",
            "filter": {
                "header": { "weak": false }
            },
            "notAField": "notAValue"
        }
        "#;
        let config = serde_json::from_str::<StreamConfigurationOptions>(json);
        assert!(config.is_ok());
    }
}
