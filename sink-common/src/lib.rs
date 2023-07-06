mod configuration;
mod connector;
mod persistence;
mod status;

use std::fmt::{self, Display};

use apibara_core::node::v1alpha2::Cursor;
use configuration::SinkConnectorFromConfigurationError;
use prost::Message;
use serde::{de, ser};
use serde_json::Value;

pub use self::configuration::{
    ConfigurationArgs, ConfigurationArgsWithoutFinality, ConfigurationError, FilterError,
    FinalityArgs, StartingCursorArgs,
};
pub use self::connector::{Sink, SinkConnector, SinkConnectorError};
pub use self::status::start_status_server;
pub use apibara_transformer::TransformerError;

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
        let options = args.to_sink_connector_options()?;
        Ok(SinkConnector::new(options))
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

/// A newtype to display a cursor that may be `None` as "genesis".
pub struct DisplayCursor<'a>(pub &'a Option<Cursor>);

impl<'a> Display for DisplayCursor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(cursor) => write!(f, "{}", cursor),
            None => write!(f, "Cursor(genesis)"),
        }
    }
}
