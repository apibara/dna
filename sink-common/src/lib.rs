mod configuration;
mod connector;
mod persistence;

use apibara_sdk::InvalidUri;
use bytesize::ByteSize;
use configuration::TransformError;
use prost::Message;
use serde::{de, ser};

pub use self::configuration::{
    ConfigurationArgs, ConfigurationError, FilterError, FinalityArgs, StartingCursorArgs,
};
pub use self::connector::{Sink, SinkConnector, SinkConnectorError};

#[derive(Debug, thiserror::Error)]
pub enum SinkConnectorFromConfigurationError {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error("Failed to parse stream URL: {0}")]
    Uri(#[from] InvalidUri),
    #[error("Failed to parse transform: {0}")]
    Transform(#[from] TransformError),
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
        let connector = SinkConnector::new(
            stream_url,
            configuration,
            transformer,
            persistence,
            max_message_size.as_u64() as usize,
        );
        Ok(connector)
    }
}
