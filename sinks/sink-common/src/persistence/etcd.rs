//! Persist state to etcd.
use apibara_core::filter::Filter;
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use etcd_client::{Client, LeaseKeeper, LockOptions, LockResponse};
use prost::Message;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use crate::{PersistedState, SinkError, SinkErrorResultExt};

use super::common::PersistenceClient;

pub struct EtcdPersistence {
    client: Client,
    sink_id: String,
    lock: Option<Lock>,
}

pub struct Lock {
    inner: LockResponse,
    lease_id: i64,
    keeper: LeaseKeeper,
    last_lock_renewal: Instant,
    min_lock_refresh_interval: Duration,
}

impl EtcdPersistence {
    pub async fn connect(
        url: &str,
        sink_id: impl Into<String>,
    ) -> Result<EtcdPersistence, SinkError> {
        let client = Client::connect([url], None)
            .await
            .persistence(&format!("failed to connect to etcd server at {url}"))?;

        Ok(EtcdPersistence {
            client,
            sink_id: sink_id.into(),
            lock: None,
        })
    }
}

#[async_trait]
impl PersistenceClient for EtcdPersistence {
    #[instrument(skip(self), level = "debug")]
    async fn lock(&mut self) -> Result<(), SinkError> {
        let lease = self
            .client
            .lease_grant(60, None)
            .await
            .persistence("failed lease grant")?;
        debug!(lease_id = %lease.id(), "acquired lease for lock");
        let (keeper, _) = self
            .client
            .lease_keep_alive(lease.id())
            .await
            .persistence("failed lease keep alive")?;

        let lock_options = LockOptions::new().with_lease(lease.id());
        let inner = self
            .client
            .lock(self.sink_id.as_str(), Some(lock_options))
            .await
            .persistence(&format!("failed lock {}", self.sink_id.as_str()))?;

        let last_lock_renewal = Instant::now();
        let min_lock_refresh_interval = Duration::from_secs(30);

        let lock = Lock {
            inner,
            lease_id: lease.id(),
            keeper,
            last_lock_renewal,
            min_lock_refresh_interval,
        };

        self.lock = Some(lock);
        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    async fn unlock(&mut self) -> Result<(), SinkError> {
        if let Some(lock) = self.lock.take() {
            self.client
                .unlock(lock.inner.key())
                .await
                .persistence("failed unlock")?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        let response = self
            .client
            .get(self.sink_id.as_str(), None)
            .await
            .persistence(&format!("failed get state {}", self.sink_id.as_str()))?;

        match response.kvs().iter().next() {
            None => Ok(PersistedState::default()),
            Some(kv) => {
                let state =
                    PersistedState::decode(kv.value()).persistence("failed to decode state")?;
                Ok(state)
            }
        }
    }

    #[instrument(skip(self), level = "trace")]
    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        self.client
            .put(self.sink_id.as_str(), state.encode_to_vec(), None)
            .await
            .persistence(&format!("failed put state {}", self.sink_id.as_str()))?;

        if let Some(lock) = self.lock.as_mut() {
            lock.keep_alive()
                .await
                .attach_printable("failed to keep lock alive")?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    async fn delete_state(&mut self) -> Result<(), SinkError> {
        self.client
            .delete(self.sink_id.as_str(), None)
            .await
            .persistence(&format!("failed delete state {}", self.sink_id.as_str()))?;
        Ok(())
    }
}

impl Lock {
    /// Sends a keep alive request.
    #[instrument(skip(self), level = "debug")]
    pub async fn keep_alive(&mut self) -> Result<(), SinkError> {
        // Renew the lock every 30 seconds to avoid hammering etcd.
        if self.last_lock_renewal.elapsed() <= self.min_lock_refresh_interval {
            return Ok(());
        }

        debug!(lease_id = %self.lease_id, "send keep alive message");
        self.keeper
            .keep_alive()
            .await
            .persistence("failed to renew lock")?;
        self.last_lock_renewal = Instant::now();

        Ok(())
    }
}
