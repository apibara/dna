//! Persist state to etcd.
use apibara_core::node::v1alpha2::Cursor;
use etcd_client::{Client, LeaseKeeper, LockOptions};
use prost::Message;
use tracing::{debug, instrument};

pub use etcd_client::LockResponse;

#[derive(Debug)]
pub struct Persistence {
    url: String,
    sink_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Etcd error: {0}")]
    Etcd(#[from] etcd_client::Error),
    #[error("Cursor decode error: {0}")]
    CursorDecode(#[from] prost::DecodeError),
}

#[derive(Clone)]
pub struct PersistenceClient {
    client: Client,
    sink_id: String,
}

pub struct Lock {
    inner: LockResponse,
    lease_id: i64,
    keeper: LeaseKeeper,
}

impl Persistence {
    pub fn new(url: impl Into<String>, sink_id: impl Into<String>) -> Self {
        Persistence {
            url: url.into(),
            sink_id: sink_id.into(),
        }
    }

    pub async fn connect(self) -> Result<PersistenceClient, PersistenceError> {
        let client = Client::connect([self.url.as_str()], None).await?;
        Ok(PersistenceClient {
            client,
            sink_id: self.sink_id,
        })
    }
}

impl PersistenceClient {
    /// Attempts to acquire a lock on the sink.
    #[instrument(skip(self), level = "debug")]
    pub async fn lock(&mut self) -> Result<Lock, PersistenceError> {
        let lease = self.client.lease_grant(60, None).await?;
        debug!(lease_id = %lease.id(), "acquired lease for lock");
        let (keeper, _) = self.client.lease_keep_alive(lease.id()).await?;

        let lock_options = LockOptions::new().with_lease(lease.id());
        let inner = self
            .client
            .lock(self.sink_id.as_str(), Some(lock_options))
            .await?;

        let lock = Lock {
            inner,
            lease_id: lease.id(),
            keeper,
        };
        Ok(lock)
    }

    /// Unlock a lock.
    #[instrument(skip(self, lock), level = "debug")]
    pub async fn unlock(&mut self, lock: Option<Lock>) -> Result<(), PersistenceError> {
        if let Some(lock) = lock {
            self.client.unlock(lock.inner.key()).await?;
        }
        Ok(())
    }

    /// Reads the currently stored cursor value.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_cursor(&mut self) -> Result<Option<Cursor>, PersistenceError> {
        let response = self.client.get(self.sink_id.as_str(), None).await?;
        match response.kvs().iter().next() {
            None => Ok(None),
            Some(kv) => {
                let cursor = Cursor::decode(kv.value())?;
                Ok(Some(cursor))
            }
        }
    }

    /// Updates the sink cursor value.
    #[instrument(skip(self), level = "trace")]
    pub async fn put_cursor(&mut self, cursor: Cursor) -> Result<(), PersistenceError> {
        self.client
            .put(self.sink_id.as_str(), cursor.encode_to_vec(), None)
            .await?;
        Ok(())
    }

    /// Deletes any stored value for the sink cursor.
    #[instrument(skip(self), level = "trace")]
    pub async fn delete_cursor(&mut self) -> Result<(), PersistenceError> {
        self.client.delete(self.sink_id.as_str(), None).await?;
        Ok(())
    }
}

impl Lock {
    /// Sends a keep alive request.
    #[instrument(skip(self), level = "debug")]
    pub async fn keep_alive(&mut self) -> Result<(), PersistenceError> {
        debug!(lease_id = %self.lease_id, "send keep alive message");
        self.keeper.keep_alive().await?;
        Ok(())
    }
}
