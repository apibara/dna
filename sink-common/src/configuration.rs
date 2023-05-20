use std::{
    fs::{self, File},
    io::BufReader,
    path::Path,
};

use apibara_core::node::v1alpha2::DataFinality;
use apibara_sdk::{
    Configuration, InvalidMetadataKey, InvalidMetadataValue, MetadataKey, MetadataMap,
    MetadataValue,
};
use clap::Args;
use jrsonnet_evaluator::{trace::PathResolver, State};
use jrsonnet_stdlib::ContextInitializer;
use prost::Message;
use serde::de;
use tracing::warn;

use crate::persistence::Persistence;

use super::connector::Transformer;

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
pub enum TransformError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Snippet evaluation error")]
    Evaluation,
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

/// Stream configuration command line flags.
#[derive(Args, Debug)]
pub struct ConfigurationArgs {
    /// Set the response preferred batch size.
    #[arg(long, env)]
    pub batch_size: Option<u64>,
    /// The json-encoded filter to use. If it starts with `@`, it is interpreted as a path to a file.
    #[arg(long, env)]
    pub filter: String,
    /// Jsonnet transformation to apply to data. If it starts with `@`, it is interpreted as a path to a file.
    #[arg(long, env)]
    pub transform: Option<String>,
    /// Limits the maximum size of a decoded message. Accept message size in human readable form,
    /// e.g. 1kb, 1MB, 1GB. If not set the default is 1MB.
    #[arg(long, env)]
    pub max_message_size: Option<String>,
    /// Add metadata to the stream, in the `key: value` format. Can be specified multiple times.
    #[arg(long, short = 'M', env)]
    pub metadata: Vec<String>,
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

impl ConfigurationArgs {
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

    /// Returns the jsonnet transformation to apply to data.
    pub fn to_transformer(&self) -> Result<Option<Transformer>, TransformError> {
        if let Some(transform) = &self.transform {
            let transform = if let Some(path) = transform.strip_prefix('@') {
                let transform_path = Path::new(path);
                fs::read_to_string(transform_path)?
            } else {
                transform.to_string()
            };

            let state = State::default();
            let stdlib = ContextInitializer::new(state.clone(), PathResolver::new_cwd_fallback());
            state.set_context_initializer(stdlib);
            let expr = state
                .evaluate_snippet("<transform>".to_owned(), transform)
                .map_err(|err| {
                    // jrsonnet Error does not implement `Send` because of an `Rc<str>` somewhere.
                    // for now, just print a warning.
                    warn!(err = ?err, "failed to evaluate snipped");
                    TransformError::Evaluation
                })?;
            let transformer = Transformer::new(state, expr);
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
