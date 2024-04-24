use std::time::Duration;

use apibara_dna_protocol::dna::stream::dna_stream_client::DnaStreamClient;
use error_stack::{Result, ResultExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::info;

use crate::{
    error::SinkError,
    filter::Filter,
    persistence::{Persistence, PersistenceClient},
    status::StatusServer,
    CursorAction, PersistedState, StatusServerClient,
};

pub struct StateManager {
    persistence: PersistenceClient,
    status_client: StatusServerClient,
}

impl StateManager {
    pub async fn start(
        mut persistence: Persistence,
        status_server: StatusServer,
        stream_client: DnaStreamClient<Channel>,
        ct: CancellationToken,
    ) -> Result<(StateManager, JoinHandle<Result<(), SinkError>>), SinkError> {
        let persistence = persistence.connect().await?;

        let (status_client, status_server) = status_server
            .clone()
            .start(stream_client, ct.clone())
            .await?;

        let status_server = tokio::spawn(status_server);

        let manager = StateManager {
            persistence,
            status_client,
        };

        Ok((manager, status_server))
    }

    pub async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        let state = self.persistence.get_state().await?;

        Ok(state)
    }

    pub async fn put_state<F: Filter>(
        &mut self,
        state: PersistedState<F>,
        action: CursorAction,
    ) -> Result<(), SinkError> {
        self.status_client
            .update_cursor(state.cursor.clone())
            .await?;

        match action {
            CursorAction::PersistAt(cursor) => {
                self.persistence
                    .put_state(PersistedState::new(Some(cursor), state.filter))
                    .await
            }
            CursorAction::Persist => self.persistence.put_state(state).await,
            CursorAction::Skip => Ok(()),
        }
    }

    pub async fn heartbeat(&mut self) -> Result<(), SinkError> {
        self.status_client.heartbeat().await?;

        Ok(())
    }

    pub async fn lock(&mut self, ct: CancellationToken) -> Result<(), SinkError> {
        info!("acquiring persistence lock");
        // lock will block until it's acquired.
        // limit the time we wait for the lock to 30 seconds or until the cancellation token is
        // cancelled.
        // notice we can straight exit if the cancellation token is cancelled, since the lock
        // is not held by us.
        tokio::select! {
            ret = self.persistence.lock() => {
                ret.change_context(SinkError::Temporary)
                    .attach_printable("failed to lock persistence")?;
                info!("lock acquired");
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                info!("failed to acquire persistence lock within 30 seconds");
                return Err(SinkError::Configuration)
                    .attach_printable("failed to acquire persistence lock within 30 seconds");
            }
            _ = ct.cancelled() => {
                return Ok(())
            }
        }

        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<(), SinkError> {
        self.persistence.unlock().await?;

        Ok(())
    }
}
