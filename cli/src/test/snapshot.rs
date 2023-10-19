use std::path::{Path, PathBuf};

use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_script::Script;
use apibara_sdk::{configuration, ClientBuilder, DataMessage};
use apibara_sink_common::{StreamConfigurationOptions, StreamOptions};
use error_stack::{Result, ResultExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_stream::StreamExt;
use tracing::debug;

use crate::{error::CliError, test::SNAPSHOTS_DIR};

#[derive(Serialize, Deserialize, Debug)]
pub struct Snapshot {
    pub script_path: PathBuf,
    pub num_batches: usize,
    pub stream_options: StreamOptions,
    pub stream_configuration_options: StreamConfigurationOptions,
    pub stream: Vec<Value>,
}

pub struct SnapshotGenerator {
    script_path: PathBuf,
    script: Script,
    num_batches: usize,
    stream_options: StreamOptions,
    stream_configuration_options: StreamConfigurationOptions,
}

pub fn get_snapshot_path(script_path: &Path) -> Result<PathBuf, CliError> {
    let file_stem = script_path
        .file_stem()
        .ok_or(CliError)
        .attach_printable_lazy(|| format!("Invalid path `{}`", script_path.display()))?;

    let current_dir = std::env::current_dir()
        .change_context(CliError)
        .attach_printable("failed to get current directory")?;

    let snapshot_path = current_dir
        .join(SNAPSHOTS_DIR)
        .join(format!("{}.json", file_stem.to_string_lossy()));

    Ok(snapshot_path)
}

impl SnapshotGenerator {
    pub fn new(
        script_path: PathBuf,
        script: Script,
        num_batches: usize,
        stream_options: StreamOptions,
        stream_configuration_options: StreamConfigurationOptions,
    ) -> Self {
        Self {
            script_path,
            script,
            num_batches,
            stream_options,
            stream_configuration_options,
        }
    }

    pub async fn generate(mut self) -> Result<Snapshot, CliError> {
        let configuration = self.stream_configuration_options.as_starknet().unwrap();
        let stream_configuration = self
            .stream_options
            .clone()
            .to_stream_configuration()
            .change_context(CliError)
            .attach_printable("failed to convert stream options")?;

        let (configuration_client, configuration_stream) = configuration::channel(128);

        configuration_client
            .send(configuration)
            .await
            .change_context(CliError)
            .attach_printable("failed to send configuration")?;

        let stream_client = ClientBuilder::default()
            .with_max_message_size(stream_configuration.max_message_size_bytes.as_u64() as usize)
            .with_metadata(stream_configuration.metadata.clone())
            .with_bearer_token(stream_configuration.bearer_token.clone())
            .connect(stream_configuration.stream_url.clone())
            .await
            .change_context(CliError)
            .attach_printable("failed to connect to DNA stream")?;

        let mut data_stream = stream_client
            .start_stream::<Filter, Block, _>(configuration_stream)
            .await
            .change_context(CliError)
            .attach_printable("failed to start DNA stream")?;

        let mut num_handled_blocks = 0;

        let mut stream: Vec<Value> = vec![];

        let mut is_empty = true;

        loop {
            tokio::select! {
                maybe_message = data_stream.try_next() => {
                    match maybe_message.change_context(CliError)? {
                        None => {
                            println!("Data stream closed");
                            break;
                        }
                        Some(message) => {
                            if num_handled_blocks >= self.num_batches {
                                break;
                            }
                            match message {
                                DataMessage::Data {
                                    cursor,
                                    end_cursor,
                                    finality,
                                    batch,
                                } => {
                                    debug!("Adding data to snapshot: {:?}-{:?}", cursor, end_cursor);
                                    let input = serde_json::to_value(&batch).change_context(CliError).attach_printable("failed to deserialize batch data")?;
                                    let output = self.script.transform(&input).await.change_context(CliError).attach_printable("failed to transform batch data")?;

                                    match &input {
                                        Value::Array(array) => {
                                            if !array.is_empty() {
                                                is_empty = false;
                                            }
                                        }
                                        Value::Object(object) => {
                                            if !object.is_empty() {
                                                is_empty = false;
                                            }
                                        }
                                        _ => is_empty = false,
                                    };

                                    stream.push(json!({
                                        "cursor": cursor,
                                        "end_cursor": end_cursor,
                                        "finality": finality,
                                        "input": input,
                                        "output": output,
                                    }));
                                }
                                DataMessage::Invalidate { cursor } => {
                                    debug!("Ignoring invalidate: {:?}", cursor);
                                }
                                DataMessage::Heartbeat => {
                                    debug!("Ignoring heartbeat");
                                }
                            }
                            num_handled_blocks += 1;
                        }
                    }
                }
            }
        }

        if is_empty {
            return Err(CliError).attach_printable("Empty snapshot, no data found for the selected options (filter, starting_block, num_batches ...)");
        }

        let stream_options = sanitize_stream_options(&self.stream_options);

        Ok(Snapshot {
            script_path: self.script_path,
            num_batches: self.num_batches,
            stream_options,
            stream_configuration_options: self.stream_configuration_options,
            stream,
        })
    }
}

/// Remove all the fields from the stream options that are not needed for the snapshot.
///
/// This is done to avoid leaking sensitive information (e.g. the bearer token) in the snapshots.
fn sanitize_stream_options(options: &StreamOptions) -> StreamOptions {
    StreamOptions {
        stream_url: options.stream_url.clone(),
        max_message_size: options.max_message_size.clone(),
        timeout_duration_seconds: options.timeout_duration_seconds,
        ..Default::default()
    }
}
