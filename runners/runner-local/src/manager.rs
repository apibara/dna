use std::fs::{create_dir_all, File};

use std::path::PathBuf;
use std::process::Stdio;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use tracing::{info, warn};

use crate::error::{LocalRunnerError, LocalRunnerResult};

use apibara_runner_common::runner::v1::Indexer;

use error_stack::ResultExt;
use tokio::process::Command;

use crate::server::IndexerInfo;
use crate::utils::{build_indexer_command, refresh_status};

pub struct IndexerManager {
    pub indexers: Arc<Mutex<HashMap<String, IndexerInfo>>>,
}

impl Default for IndexerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexerManager {
    pub fn new() -> Self {
        Self {
            indexers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_indexer(
        &self,
        indexer_id: String,
        indexer: Indexer,
    ) -> LocalRunnerResult<()> {
        let command = build_indexer_command(&indexer_id, &indexer)?;

        let port = portpicker::pick_unused_port()
            .ok_or(LocalRunnerError::Internal)
            .attach_printable("Can't pick a port for the status server")?;

        let status_server_address = format!("0.0.0.0:{port}");

        let status_server_args = &[
            "--status-server-address".to_string(),
            status_server_address.clone(),
        ];

        let status_server_address = format!("http://{status_server_address}");

        let cmd = format!(
            "{} {} {}",
            command.program,
            command.args.join(" "),
            status_server_args.join(" ")
        );
        info!("Starting indexer {} with command `{}`", indexer.name, cmd);

        let logs_dir = "logs";

        let (stdout, stderr) = create_dir_all(logs_dir)
            .change_context(LocalRunnerError::Internal)
            .attach_printable("cannot create logs folder")
            .and_then(|_| {
                let stdout_path = PathBuf::from(format!("{logs_dir}/{}-stdout.log", indexer.name));
                let stderr_path = PathBuf::from(format!("{logs_dir}/{}-stderr.log", indexer.name));

                let stdout_file = File::create(&stdout_path)
                    .change_context(LocalRunnerError::Internal)
                    .attach_printable(format!(
                        "cannot create stdout file `{}`",
                        stdout_path.display()
                    ))?;

                let stderr_file = File::create(&stderr_path)
                    .change_context(LocalRunnerError::Internal)
                    .attach_printable(format!(
                        "cannot create stderr file `{}`",
                        stderr_path.display()
                    ))?;

                let stdout = Stdio::from(stdout_file);
                let stderr = Stdio::from(stderr_file);

                info!(
                    "Writing indexer {} stdout to `{}`",
                    indexer.name,
                    &stdout_path.display()
                );
                info!(
                    "Writing indexer {} stderr to `{}`",
                    indexer.name,
                    &stderr_path.display()
                );

                Ok((stdout, stderr))
            })
            .unwrap_or_else(|err| {
                warn!(err =? err, "failed to write indexer logs in directory `{}`", logs_dir);
                (Stdio::piped(), Stdio::piped())
            });

        let child = Command::new(command.program)
            .current_dir(command.current_dir)
            .envs(command.envs)
            .args(command.args)
            .args(status_server_args)
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .change_context(LocalRunnerError::Internal)
            .attach_printable("failed to spawn indexer")?;

        // TODO: handle NotFound command which means the plugin is not installed separately from other errors
        // TODO: check first that it really started, like a wrong or missing cli argument

        let indexer_name = indexer.name.clone();

        let indexer_info = IndexerInfo {
            indexer_id,
            indexer,
            child,
            status_server_address,
        };

        self.indexers
            .lock()
            .await
            .insert(indexer_name, indexer_info);
        Ok(())
    }

    pub async fn refresh_status(&self, name: &str) -> LocalRunnerResult<()> {
        let mut indexers = self.indexers.lock().await;

        let indexer_info = indexers
            .get_mut(name)
            .ok_or(LocalRunnerError::NotFound(name.to_string()))
            .attach_printable(format!("indexer {name} not found"))?;

        refresh_status(indexer_info).await?;

        Ok(())
    }

    pub async fn refresh_status_all(&self) -> LocalRunnerResult<()> {
        let mut indexers = self.indexers.lock().await;

        let mut results = Vec::new();
        for indexer_info in indexers.values_mut() {
            results.push(refresh_status(indexer_info).await);
        }

        results.into_iter().collect()
    }

    pub async fn get_indexer(&self, name: &str) -> LocalRunnerResult<Indexer> {
        let mut indexers = self.indexers.lock().await;

        let indexer_info = indexers
            .get_mut(name)
            .ok_or(LocalRunnerError::NotFound(name.to_string()))
            .attach_printable(format!("indexer {name} not found"))?;

        Ok(indexer_info.indexer.clone())
    }

    pub async fn delete_indexer(&self, name: &str) -> LocalRunnerResult<()> {
        let mut indexers = self.indexers.lock().await;
        let indexer_info = indexers
            .get_mut(name)
            .ok_or(LocalRunnerError::NotFound(name.to_string()))
            .attach_printable(format!("indexer {} not found", name))?;

        let indexer_running = indexer_info
            .child
            .try_wait()
            .change_context(LocalRunnerError::Internal)
            .attach_printable("failed to check status of indexer process")?
            .is_none();

        if indexer_running {
            indexer_info
                .child
                .kill()
                .await
                .change_context(LocalRunnerError::Internal)
                .attach_printable("failed to kill process")?;
        }

        indexers.remove(name);

        Ok(())
    }

    pub async fn list_indexers(&self) -> LocalRunnerResult<Vec<Indexer>> {
        let indexers = self.indexers.lock().await;

        Ok(indexers
            .values()
            .map(|indexer_info| indexer_info.indexer.clone())
            .collect())
    }

    pub async fn has_indexer(&self, name: &str) -> bool {
        let indexers = self.indexers.lock().await;
        indexers.contains_key(name)
    }
}
