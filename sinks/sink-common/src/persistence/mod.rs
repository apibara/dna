pub mod common;
mod default;
mod fs;
mod redis;

pub use self::common::{PersistedState, PersistenceClient as PersistenceClientTrait};
pub use self::default::InMemoryPersistence;
pub use self::fs::DirPersistence;
pub use self::redis::RedisPersistence;

use async_trait::async_trait;
use error_stack::Result;

use crate::configuration::PersistenceOptions;
use crate::filter::Filter;
use crate::SinkError;

/// Persistence client factory.
pub struct Persistence {
    options: PersistenceOptions,
}

impl Persistence {
    pub fn new_from_options(options: PersistenceOptions) -> Self {
        Self { options }
    }

    pub async fn connect(&mut self) -> Result<PersistenceClient, SinkError> {
        let sink_id = self
            .options
            .sink_id
            .clone()
            .unwrap_or_else(|| "default".to_string());

        if let Some(dir_path) = &self.options.persistence_type.persist_to_fs {
            let persistence = DirPersistence::initialize(dir_path, sink_id)?;
            Ok(PersistenceClient::new_dir(persistence))
        } else if let Some(redis_url) = &self.options.persistence_type.persist_to_redis {
            let client = redis::RedisPersistence::connect(redis_url, sink_id).await?;
            Ok(PersistenceClient::new_redis(client))
        } else {
            Ok(PersistenceClient::new_in_memory())
        }
    }
}

pub enum PersistenceClient {
    Dir(DirPersistence),
    Redis(RedisPersistence),
    InMemory(InMemoryPersistence),
}

impl PersistenceClient {
    pub fn new_dir(inner: DirPersistence) -> Self {
        Self::Dir(inner)
    }

    fn new_redis(inner: RedisPersistence) -> PersistenceClient {
        Self::Redis(inner)
    }

    pub fn new_in_memory() -> Self {
        Self::InMemory(InMemoryPersistence::default())
    }

    pub async fn lock(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Dir(inner) => inner.lock().await,
            Self::Redis(inner) => inner.lock().await,
            Self::InMemory(inner) => inner.lock().await,
        }
    }

    pub async fn unlock(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Dir(inner) => inner.unlock().await,
            Self::Redis(inner) => inner.unlock().await,
            Self::InMemory(inner) => inner.unlock().await,
        }
    }

    pub async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        match self {
            Self::Dir(inner) => inner.get_state().await,
            Self::Redis(inner) => inner.get_state().await,
            Self::InMemory(inner) => inner.get_state().await,
        }
    }

    pub async fn put_state<F: Filter>(
        &mut self,
        state: PersistedState<F>,
    ) -> Result<(), SinkError> {
        match self {
            Self::Dir(inner) => inner.put_state(state).await,
            Self::Redis(inner) => inner.put_state(state).await,
            Self::InMemory(inner) => inner.put_state(state).await,
        }
    }

    pub async fn delete_state(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Dir(inner) => inner.delete_state().await,
            Self::Redis(inner) => inner.delete_state().await,
            Self::InMemory(inner) => inner.delete_state().await,
        }
    }
}

#[async_trait]
impl PersistenceClientTrait for PersistenceClient {
    async fn lock(&mut self) -> Result<(), SinkError> {
        self.lock().await
    }

    async fn unlock(&mut self) -> Result<(), SinkError> {
        self.unlock().await
    }

    async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        self.get_state().await
    }

    async fn put_state<F: Filter>(&mut self, state: PersistedState<F>) -> Result<(), SinkError> {
        self.put_state(state).await
    }

    async fn delete_state(&mut self) -> Result<(), SinkError> {
        self.delete_state().await
    }
}
