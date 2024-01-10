pub mod common;
mod default;
mod etcd;
mod fs;

pub use self::common::{PersistedState, PersistenceClient as PersistenceClientTrait};
pub use self::default::NoPersistence;
pub use self::etcd::EtcdPersistence;
pub use self::fs::DirPersistence;

use apibara_core::filter::Filter;
use async_trait::async_trait;
use error_stack::Result;

use crate::configuration::PersistenceOptions;
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
        if let Some(etcd_url) = &self.options.persistence_type.persist_to_etcd {
            let client = etcd::EtcdPersistence::connect(etcd_url, sink_id).await?;
            Ok(PersistenceClient::new_etcd(client))
        } else if let Some(dir_path) = &self.options.persistence_type.persist_to_fs {
            let persistence = DirPersistence::initialize(dir_path, sink_id)?;
            Ok(PersistenceClient::new_dir(persistence))
        } else {
            Ok(PersistenceClient::new_none())
        }
    }
}

pub enum PersistenceClient {
    Etcd(EtcdPersistence),
    Dir(DirPersistence),
    None(NoPersistence),
}

impl PersistenceClient {
    pub fn new_etcd(inner: EtcdPersistence) -> Self {
        Self::Etcd(inner)
    }

    pub fn new_dir(inner: DirPersistence) -> Self {
        Self::Dir(inner)
    }

    pub fn new_none() -> Self {
        Self::None(NoPersistence)
    }

    pub async fn lock(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Etcd(inner) => inner.lock().await,
            Self::Dir(inner) => inner.lock().await,
            Self::None(inner) => inner.lock().await,
        }
    }

    pub async fn unlock(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Etcd(inner) => inner.unlock().await,
            Self::Dir(inner) => inner.unlock().await,
            Self::None(inner) => inner.unlock().await,
        }
    }

    pub async fn get_state<F: Filter>(&mut self) -> Result<PersistedState<F>, SinkError> {
        match self {
            Self::Etcd(inner) => inner.get_state().await,
            Self::Dir(inner) => inner.get_state().await,
            Self::None(inner) => inner.get_state().await,
        }
    }

    pub async fn put_state<F: Filter>(
        &mut self,
        state: PersistedState<F>,
    ) -> Result<(), SinkError> {
        match self {
            Self::Etcd(inner) => inner.put_state(state).await,
            Self::Dir(inner) => inner.put_state(state).await,
            Self::None(inner) => inner.put_state(state).await,
        }
    }

    pub async fn delete_state(&mut self) -> Result<(), SinkError> {
        match self {
            Self::Etcd(inner) => inner.delete_state().await,
            Self::Dir(inner) => inner.delete_state().await,
            Self::None(inner) => inner.delete_state().await,
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
