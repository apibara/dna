mod configuration;
mod connector;
mod persistence;

use apibara_sdk::InvalidUri;
use bytesize::ByteSize;
use configuration::MetadataError;
use prost::Message;
use serde::{de, ser};
use serde_json::Value;

pub use self::configuration::{
    ConfigurationArgs, ConfigurationArgsWithoutFinality, ConfigurationError, FilterError,
    FinalityArgs, StartingCursorArgs,
};
pub use self::connector::{Sink, SinkConnector, SinkConnectorError};
pub use apibara_transformer::TransformerError;

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorFromConfigurationError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("Failed to parse stream URL: {0}")]
    Uri(#[from] InvalidUri),
    #[error("Failed to parse transform: {0}")]
    Transform(#[from] TransformerError),
    #[error("Failed to message size: {0}")]
    SizeConversion(String),
}

pub trait SinkConnectorExt: Sized {
    fn from_configuration_args(
        args: ConfigurationArgs,
    ) -> Result<Self, SinkConnectorFromConfigurationError>;
}

impl<F, B> SinkConnectorExt for SinkConnector<F, B>
where
    F: Message + Default + Clone + de::DeserializeOwned,
    B: Message + Default + ser::Serialize,
{
    fn from_configuration_args(
        args: ConfigurationArgs,
    ) -> Result<Self, SinkConnectorFromConfigurationError> {
        let max_message_size: ByteSize = args
            .max_message_size
            .as_ref()
            .map(|s| s.parse())
            .transpose()
            .map_err(SinkConnectorFromConfigurationError::SizeConversion)?
            .unwrap_or(ByteSize::mb(1));
        let configuration = args.to_configuration::<F>()?;
        let stream_url = args.stream_url.parse()?;
        let transformer = args.to_transformer()?;
        let persistence = args.to_persistence();
        let metadata = args.to_metadata()?;
        let connector = SinkConnector::new(
            stream_url,
            configuration,
            transformer,
            metadata,
            persistence,
            max_message_size.as_u64() as usize,
        );
        Ok(connector)
    }
}

pub fn is_array_of_objects(value: &Value) -> bool {
    match value {
        Value::Array(array) => {
            for v in array {
                if !v.is_object() {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}
