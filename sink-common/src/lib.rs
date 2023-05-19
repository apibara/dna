mod configuration;
mod connector;
mod persistence;

use apibara_sdk::InvalidUri;
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
        let configuration = args.to_configuration::<F>()?;
        let stream_url = args.stream_url.parse()?;
        let transformer = args.to_transformer()?;
        let persistence = args.to_persistence();
        let connector = SinkConnector::new(stream_url, configuration, transformer, persistence);
        Ok(connector)
    }
}
