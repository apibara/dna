mod cli;
mod configuration;
mod connector;
mod cursor;
mod error;
mod json;
mod persistence;
mod status;

use std::env;

use apibara_core::starknet::v1alpha2;
use error_stack::Result;
use error_stack::ResultExt;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub use self::cli::*;
pub use self::configuration::*;
pub use self::connector::*;
pub use self::cursor::DisplayCursor;
pub use self::error::*;
pub use self::json::ValueExt;
pub use self::persistence::*;
pub use self::status::*;
pub use apibara_sink_options_derive::SinkOptions;

pub use apibara_script::ScriptOptions;

#[derive(Debug, Deserialize)]
pub struct FullOptionsFromScript<SinkOptions> {
    #[serde(flatten)]
    pub connector: OptionsFromScript,
    #[serde(flatten)]
    pub sink: SinkOptions,
}

pub async fn run_sink_connector<S>(
    script: &str,
    connector_cli_options: OptionsFromCli,
    sink_cli_options: S::Options,
    ct: CancellationToken,
) -> Result<(), SinkConnectorError>
where
    S: Sink + Send + Sync,
{
    let script_allowed_env_vars =
        load_environment_variables(&connector_cli_options.connector.dotenv)?;

    let script_options = ScriptOptions {
        allow_env: script_allowed_env_vars,
    };

    let mut script = load_script(script, script_options)
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("failed to load script")?;

    let options_from_script = script
        .configuration::<FullOptionsFromScript<S::Options>>()
        .await
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("failed to load configuration from script")?;

    script
        .check_transform_is_exported()
        .await
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("missing or invalid transform function")?;

    // Setup sink.
    let sink_options = sink_cli_options.merge(options_from_script.sink);
    let sink = S::from_options(sink_options)
        .await
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("invalid sink options")?;

    // Setup connector.
    let connector_options_from_script = options_from_script.connector;
    let stream_configuration = connector_options_from_script.stream_configuration;
    let stream_options = connector_cli_options
        .stream
        .merge(connector_options_from_script.stream);

    let stream = stream_options
        .to_stream_configuration()
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("invalid stream options")?;

    let persistence = Persistence::new_from_options(connector_cli_options.connector.persistence);
    let status_server = connector_cli_options
        .connector
        .status_server
        .to_status_server()
        .change_context(SinkConnectorError::Configuration)
        .attach_printable("invalid status server options")?;

    let sink_connector_options = SinkConnectorOptions {
        stream,
        persistence,
        status_server,
    };

    let connector = SinkConnector::new(script, sink, sink_connector_options);

    if let Some(starknet_config) = stream_configuration.as_starknet() {
        connector
            .consume_stream::<v1alpha2::Filter, v1alpha2::Block>(starknet_config, ct)
            .await
            .attach_printable("error while streaming data")?;
    } else {
        todo!()
    };

    Ok(())
}

pub fn load_environment_variables(
    options: &DotenvOptions,
) -> Result<Option<Vec<String>>, SinkConnectorError> {
    let Some(allow_env) = options.allow_env.as_ref() else {
        return Ok(None);
    };

    let env_iter = dotenvy::from_path_iter(allow_env)
        .change_context(SinkConnectorError::Configuration)
        .attach_printable_lazy(|| {
            format!(
                "failed to load environment variables from path: {:?}",
                allow_env
            )
        })?;

    let mut allow_env = vec![];
    for item in env_iter {
        let (key, value) = item
            .change_context(SinkConnectorError::Configuration)
            .attach_printable("invalid environment variable")?;
        allow_env.push(key.clone());
        debug!(env = ?key, "allowing environment variable");
        env::set_var(key, value);
    }

    if allow_env.is_empty() {
        return Ok(None);
    }

    Ok(Some(allow_env))
}
