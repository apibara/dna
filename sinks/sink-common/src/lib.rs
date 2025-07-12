mod cli;
mod configuration;
mod connector;
mod cursor;
mod error;
mod json;
pub mod persistence;
mod sink;
mod status;

use apibara_core::starknet::v1alpha2;
use error_stack::Result;
use error_stack::ResultExt;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

pub use self::cli::*;
pub use self::configuration::*;
pub use self::connector::*;
pub use self::cursor::DisplayCursor;
pub use self::error::*;
pub use self::json::ValueExt;
pub use self::persistence::*;
pub use self::sink::*;
pub use self::status::*;
pub use apibara_sink_options_derive::SinkOptions;

pub use apibara_script::ScriptOptions as IndexerOptions;

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
) -> Result<(), SinkError>
where
    S: Sink + Send + Sync,
{
    let script_options = connector_cli_options
        .connector
        .script
        .load_environment_variables()
        .map_err(|err| err.configuration("failed to parse cli options"))?
        .into_indexer_options();

    let mut script = load_script(script, script_options)
        .map_err(|err| err.configuration("failed to load script"))?;

    let options_from_script = script
        .configuration::<FullOptionsFromScript<S::Options>>()
        .await
        .map_err(|err| err.configuration("failed to load configuration from script"))?;

    script
        .check_transform_is_exported()
        .await
        .map_err(|err| err.configuration("missing or invalid transform function"))?;

    // Setup sink.
    let sink_options = sink_cli_options.merge(options_from_script.sink);
    let sink = S::from_options(sink_options)
        .await
        .map_err(|err| err.configuration("invalid sink options"))?;

    // Setup connector.
    let connector_options_from_script = options_from_script.connector;
    let stream_configuration = connector_options_from_script.stream_configuration;
    let stream_options = connector_cli_options
        .stream
        .merge(connector_options_from_script.stream);

    let stream = stream_options
        .to_stream_configuration()
        .map_err(|err| err.configuration("invalid stream options"))?;

    let persistence = Persistence::new_from_options(connector_cli_options.connector.persistence);
    let status_server = connector_cli_options
        .connector
        .status_server
        .to_status_server()
        .map_err(|err| err.configuration("invalid status server options"))?;

    let backoff = connector_cli_options.backoff.to_backoff();
    let sink_connector_options = SinkConnectorOptions {
        stream,
        persistence,
        status_server,
        backoff,
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
