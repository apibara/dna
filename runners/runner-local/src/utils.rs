use std::collections::HashMap;

use tokio::time::timeout;

use crate::error::{LocalRunnerError, LocalRunnerResult, LocalRunnerResultExt};

use apibara_runner_common::runner::v1::{source::Source, Filesystem, Indexer, State};
use apibara_sink_common::{GetStatusRequest, StatusClient};

use crate::server::IndexerInfo;

pub struct CommandWrapper {
    pub program: String,
    pub args: Vec<String>,
    pub current_dir: String,
    pub envs: HashMap<String, String>,
}

pub fn build_indexer_command(
    indexer_id: &str,
    indexer: &Indexer,
) -> LocalRunnerResult<CommandWrapper> {
    let envs_list = indexer
        .environment
        .keys()
        .map(|s| s as &str)
        .collect::<Vec<&str>>()
        .join(",");

    let source = indexer
        .source
        .clone()
        .and_then(|source| source.source)
        .ok_or(LocalRunnerError::missing_argument("indexer.source"))?;

    let (current_dir, script_path) = if let Source::Filesystem(Filesystem { path, script }) = source
    {
        (path, script)
    } else {
        return Err(LocalRunnerError::invalid_argument(
            "indexer.source",
            "only Filesystem source is supported",
        ));
    };

    let args = vec![
        "run".to_string(),
        script_path,
        "--sink-id".to_string(),
        indexer_id.to_string(),
        "--allow-env-from-env".to_string(),
        envs_list,
    ];

    let program = format!("apibara-sink-{}", indexer.sink_type);

    let envs = indexer.environment.clone();

    let command = CommandWrapper {
        program,
        args,
        current_dir,
        envs,
    };

    Ok(command)
}

pub async fn refresh_status(indexer_info: &mut IndexerInfo) -> LocalRunnerResult<()> {
    let (status, message) = match indexer_info
        .child
        .try_wait()
        .internal("failed to check status of indexer process")?
    {
        Some(status) => match status.code() {
            Some(78) => (78, "indexer failed: configuration"),
            Some(75) => (75, "indexer failed: temporary"),
            Some(code) => (code, "indexer failed: fatal"),
            None => (-1, "indexer exit by signal"),
        },
        None => (-2, "indexer running"),
    };

    let message = message.to_string();

    let state = indexer_info.indexer.state.take().unwrap_or_default();

    let state = State {
        status,
        message,
        ..state
    };

    // Update the state with the the process status first
    // This way, if it fails to reach the status server, we still refresh the process status
    indexer_info.indexer.state = Some(state);

    // TODO: Investigate why it hangs here more than the timeout and sometimes even forever,
    // this blocks everything because of indexer mutex being locked
    // It happened when I shut down the mongo db, it hangs forever until I quit the server,
    // then it returned the list without the status server information
    let duration = std::time::Duration::from_secs(1);
    let mut status_client = timeout(
        duration,
        StatusClient::connect(indexer_info.status_server_address.clone()),
    )
    .await
    .internal("failed to connect to status server: timeout")?
    .internal("failed to connect to status server")?;

    let response = status_client
        .get_status(GetStatusRequest {})
        .await
        .internal("failed to get status")?;

    let response = response.into_inner();
    let current_block = response.current_block();
    let head_block = response.head_block();

    let state = indexer_info.indexer.state.take().unwrap_or_default();

    let state = State {
        current_block,
        head_block,
        ..state
    };

    indexer_info.indexer.state = Some(state);

    Ok(())
}
