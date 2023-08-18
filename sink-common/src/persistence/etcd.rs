//! Persist state to etcd.
use apibara_core::node::v1alpha2::Cursor;
use async_trait::async_trait;
use etcd_client::{Client, LeaseKeeper, LockOptions, LockResponse};
use prost::Message;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

use super::common::{PersistenceClient, PersistenceClientError};

#[derive(Debug, thiserror::Error)]
pub enum EtcdPersistenceError {
    #[error("Etcd error: {0}")]
    Etcd(#[from] etcd_client::Error),
    #[error("Cursor decode error: {0}")]
    CursorDecode(#[from] prost::DecodeError),
}

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
    ) -> Result<EtcdPersistence, EtcdPersistenceError> {
        let client = Client::connect([url], None).await?;
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
            .map_err(PersistenceClientError::lock)?;
        debug!(lease_id = %lease.id(), "acquired lease for lock");
        let (keeper, _) = self
            .client
            .lease_keep_alive(lease.id())
            .await
            .map_err(PersistenceClientError::lock)?;

        let lock_options = LockOptions::new().with_lease(lease.id());
        let inner = self
            .client
            .lock(self.sink_id.as_str(), Some(lock_options))
            .await
            .map_err(PersistenceClientError::lock)?;

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
                .map_err(PersistenceClientError::unlock)?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceClientError> {
        let response = self
            .client
            .get(self.sink_id.as_str(), None)
            .await
            .map_err(PersistenceClientError::get_cursor)?;

        match response.kvs().iter().next() {
            None => Ok(None),
            Some(kv) => {
                let cursor =
                    Cursor::decode(kv.value()).map_err(PersistenceClientError::get_cursor)?;
                Ok(Some(cursor))
            }
        }
    }

    #[instrument(skip(self), level = "trace")]
    async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), PersistenceClientError> {
        self.client
            .put(self.sink_id.as_str(), cursor.encode_to_vec(), None)
            .await
            .map_err(PersistenceClientError::put_cursor)?;

        if let Some(lock) = self.lock.as_mut() {
            lock.keep_alive()
                .await
                .map_err(PersistenceClientError::put_cursor)?;
        }

        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    async fn delete_cursor(&mut self) -> Result<(), PersistenceClientError> {
        self.client
            .delete(self.sink_id.as_str(), None)
            .await
            .map_err(PersistenceClientError::delete_cursor)?;
        Ok(())
    }
}

impl Lock {
    /// Sends a keep alive request.
    #[instrument(skip(self), level = "debug")]
    pub async fn keep_alive(&mut self) -> Result<(), EtcdPersistenceError> {
        // Renew the lock every 30 seconds to avoid hammering etcd.
        if self.last_lock_renewal.elapsed() <= self.min_lock_refresh_interval {
            return Ok(());
        }

        debug!(lease_id = %self.lease_id, "send keep alive message");
        self.keeper.keep_alive().await?;
        self.last_lock_renewal = Instant::now();

        Ok(())
    }
}
