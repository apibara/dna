mod configuration;
mod connector;

use apibara_sdk::InvalidUri;
use prost::Message;
use serde::de;

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
}

pub trait SinkConnectorExt: Sized {
    fn from_configuration_args(
        args: ConfigurationArgs,
    ) -> Result<Self, SinkConnectorFromConfigurationError>;
}

impl<F, B> SinkConnectorExt for SinkConnector<F, B>
where
    F: Message + Default + Clone + de::DeserializeOwned,
    B: Message + Default,
{
    fn from_configuration_args(
        args: ConfigurationArgs,
    ) -> Result<Self, SinkConnectorFromConfigurationError> {
        let configuration = args.to_configuration::<F>()?;
        let stream_url = args.stream_url.parse()?;
        let connector = SinkConnector::new(stream_url, configuration);
        Ok(connector)
    }
}
