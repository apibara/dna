mod common;
mod default;
mod etcd;
mod fs;

pub use self::common::{PersistenceClient, PersistenceClientError};
pub use self::default::NoPersistence;
pub use self::etcd::{EtcdPersistence, EtcdPersistenceError};
pub use self::fs::DirPersistence;

use crate::configuration::PersistenceOptions;

/// Persistence client factory.
pub struct Persistence {
    options: PersistenceOptions,
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Failed to connect to etcd: ")]
    EtcdError(#[from] EtcdPersistenceError),
    #[error("Failed to create persistence directory: ")]
    FsError(#[from] std::io::Error),
}

impl Persistence {
    pub fn new_from_options(options: PersistenceOptions) -> Self {
        Self { options }
    }

    pub async fn connect(&mut self) -> Result<Box<dyn PersistenceClient + Send>, PersistenceError> {
        let sink_id = self
            .options
            .sink_id
            .clone()
            .unwrap_or_else(|| "default".to_string());
        if let Some(etcd_url) = &self.options.persistence_type.persist_to_etcd {
            let client = etcd::EtcdPersistence::connect(etcd_url, sink_id).await?;
            Ok(Box::new(client))
        } else if let Some(dir_path) = &self.options.persistence_type.persist_to_fs {
            let persistence = DirPersistence::initialize(dir_path, sink_id)?;
            Ok(Box::new(persistence))
        } else {
            Ok(Box::new(NoPersistence))
        }
    }
}
