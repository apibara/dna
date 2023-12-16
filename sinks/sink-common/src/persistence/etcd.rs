//! Persist state to etcd.
use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use etcd_client::{Client, LeaseKeeper, LockOptions, LockResponse};
use prost::Message;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use super::common::{PersistenceClient, PersistenceClientError, PersistenceClientResultExt};

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
    ) -> Result<EtcdPersistence, PersistenceClientError> {
        let client = Client::connect([url], None)
            .await
            .persistence_client_error(&format!("failed to connect to etcd server at {url}"))?;

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
    async fn lock(&mut self) -> Result<(), PersistenceClientError> {
        let lease = self
            .client
            .lease_grant(60, None)
            .await
            .persistence_client_error("failed lease grant")?;
        debug!(lease_id = %lease.id(), "acquired lease for lock");
        let (keeper, _) = self
            .client
            .lease_keep_alive(lease.id())
            .await
            .persistence_client_error("failed lease keep alive")?;

        let lock_options = LockOptions::new().with_lease(lease.id());
        let inner = self
            .client
            .lock(self.sink_id.as_str(), Some(lock_options))
            .await
            .persistence_client_error(&format!("failed lock {}", self.sink_id.as_str()))?;

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
    async fn unlock(&mut self) -> Result<(), PersistenceClientError> {
        if let Some(lock) = self.lock.take() {
            self.client
                .unlock(lock.inner.key())
                .await
                .persistence_client_error("failed unlock")?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceClientError> {
        let response = self
            .client
            .get(self.sink_id.as_str(), None)
            .await
            .persistence_client_error(&format!("failed get cursor {}", self.sink_id.as_str()))?;

        match response.kvs().iter().next() {
            None => Ok(None),
            Some(kv) => {
                let cursor = Cursor::decode(kv.value())
                    .persistence_client_error("failed to decode cursor")?;
                Ok(Some(cursor))
            }
        }
    }

    #[instrument(skip(self), level = "trace")]
    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), PersistenceClientError> {
        self.client
            .put(self.sink_id.as_str(), cursor.encode_to_vec(), None)
            .await
            .persistence_client_error(&format!("failed put cursor {}", self.sink_id.as_str()))?;

        if let Some(lock) = self.lock.as_mut() {
            lock.keep_alive()
                .await
                .attach_printable("failed to keep lock alive")?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    async fn delete_cursor(&mut self) -> Result<(), PersistenceClientError> {
        self.client
            .delete(self.sink_id.as_str(), None)
            .await
            .persistence_client_error(&format!("failed delete cursor {}", self.sink_id.as_str()))?;
        Ok(())
    }
}

impl Lock {
    /// Sends a keep alive request.
    #[instrument(skip(self), level = "debug")]
    pub async fn keep_alive(&mut self) -> Result<(), PersistenceClientError> {
        // Renew the lock every 30 seconds to avoid hammering etcd.
        if self.last_lock_renewal.elapsed() <= self.min_lock_refresh_interval {
            return Ok(());
        }

        debug!(lease_id = %self.lease_id, "send keep alive message");
        self.keeper
            .keep_alive()
            .await
            .change_context(PersistenceClientError)?;
        self.last_lock_renewal = Instant::now();

        Ok(())
    }
}
