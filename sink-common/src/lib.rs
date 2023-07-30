mod cli;
mod configuration;
mod connector;
mod cursor;
mod json;
mod persistence;
mod status;

use std::env;

use apibara_core::starknet::v1alpha2;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

pub use self::cli::*;
pub use self::configuration::*;
pub use self::connector::*;
pub use self::cursor::DisplayCursor;
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

    let mut script = load_script(script, script_options)?;

    let options_from_script = script
        .configuration::<FullOptionsFromScript<S::Options>>()
        .await?;

    // Setup sink.
    let sink_options = options_from_script.sink.merge(sink_cli_options);
    let sink = S::from_options(sink_options).await.unwrap();

    // Setup connector.
    let connector_options_from_script = options_from_script.connector;
    let stream_configuration = connector_options_from_script.stream_configuration;
    let stream_options = connector_options_from_script
        .stream
        .merge(connector_cli_options.stream);

    let stream = stream_options.to_stream_configuration().unwrap();
    let persistence = connector_cli_options
        .connector
        .persistence
        .map(|p| p.to_persistence())
        .transpose()
        .unwrap();
    let status_server = connector_cli_options
        .connector
        .status_server
        .to_status_server()
        .unwrap();

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
            .unwrap();
    } else {
        todo!()
    };

    Ok(())
}

pub fn load_environment_variables(
    options: &DotenvOptions,
) -> Result<Option<Vec<String>>, SinkConnectorError> {
    let Some(env_from_file) = options.env_from_file.as_ref() else {
        return Ok(None);
    };

    let env_iter = dotenvy::from_path_iter(env_from_file)?;

    let mut allow_env = vec![];
    for item in env_iter {
        let (key, value) = item?;
        allow_env.push(key.clone());
        env::set_var(key, value);
    }

    if allow_env.is_empty() {
        return Ok(None);
    }

    Ok(Some(allow_env))
}
