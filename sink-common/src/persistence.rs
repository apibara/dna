//! Persist state to etcd.
use etcd_client::Client;

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
}

pub struct PersistenceClient {
    client: Client,
    sink_id: String,
}

pub struct Lock<'a> {
    client: &'a mut PersistenceClient,
    inner: LockResponse,
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
    pub async fn lock(&mut self) -> Result<Lock<'_>, PersistenceError> {
        let inner = self.client.lock(self.sink_id.as_str(), None).await?;
        let lock = Lock {
            client: self,
            inner,
        };
        Ok(lock)
    }

    async fn unlock(&mut self, key: &[u8]) -> Result<(), PersistenceError> {
        println!("unlock");
        self.client.unlock(key).await?;
        Ok(())
    }
}

impl<'a> Lock<'a> {
    pub async fn unlock(self) -> Result<(), PersistenceError> {
        self.client.unlock(self.inner.key()).await?;
        Ok(())
    }
}
