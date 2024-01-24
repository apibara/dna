use apibara_core::filter::Filter;
use async_trait::async_trait;
use error_stack::Result;
use redis::Commands;
use tracing::warn;

use crate::{common::PersistenceClient, PersistedState, SinkError, SinkErrorResultExt};

pub struct RedisPersistence {
    client: redis::Client,
    sink_id: String,
}

impl RedisPersistence {
    pub async fn connect(
        url: &str,
        sink_id: impl Into<String>,
    ) -> Result<RedisPersistence, SinkError> {
        let client = redis::Client::open(url)
            .persistence(&format!("failed to connect to redis server at {url}"))?;

        Ok(RedisPersistence {
            client,
            sink_id: sink_id.into(),
        })
    }
}

#[async_trait]
impl PersistenceClient for RedisPersistence {
    async fn lock(&mut self) -> Result<(), SinkError> {
        warn!("Locking is not yet supported for Redis persistence.");
        Ok(())
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        warn!("Locking is not yet supported for Redis persistence.");
        Ok(())
    }

    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        let mut conn = self
            .client
            .get_connection()
            .persistence("failed to connect to redis")?;

        let content = conn
            .get::<_, Option<String>>(&self.sink_id)
            .persistence("failed to get state from redis")?;

        match content {
            Some(content) => {
                Ok(serde_json::from_str(&content).persistence("failed to deserialize state")?)
            }
            None => Ok(PersistedState::<F>::default()),
        }
    }

    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        let mut conn = self
            .client
            .get_connection()
            .persistence("failed to connect to redis")?;

        let serialized = serde_json::to_string(&state).persistence("failed to serialize state")?;

        conn.set(&self.sink_id, serialized)
            .persistence("failed to put state in redis")?;

        Ok(())
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        let mut conn = self
            .client
            .get_connection()
            .persistence("failed to connect to redis")?;

        conn.del(&self.sink_id)
            .persistence("failed to delete state from redis")?;

        Ok(())
    }
}
