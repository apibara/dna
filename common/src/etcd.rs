use error_stack::{Result, ResultExt};

use crate::utils::normalize_prefix;

pub use etcd_client::{GetResponse, PutResponse, StatusResponse};

#[derive(Debug)]
pub struct EtcdClientError;

#[derive(Debug)]
pub struct EtcdClientOptions {
    pub prefix: Option<String>,
}

#[derive(Clone)]
pub struct EtcdClient {
    client: etcd_client::Client,
    prefix: String,
}

#[derive(Clone)]
pub struct KvClient {
    client: etcd_client::KvClient,
    prefix: String,
}

impl EtcdClient {
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: EtcdClientOptions,
    ) -> Result<Self, EtcdClientError> {
        let client = etcd_client::Client::connect(endpoints, None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to connect to etcd")?;

        let prefix = normalize_prefix(options.prefix);

        Ok(Self { client, prefix })
    }

    pub async fn status(&mut self) -> Result<StatusResponse, EtcdClientError> {
        self.client
            .status()
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get etcd status")
    }

    pub fn kv_client(&self) -> KvClient {
        KvClient {
            client: self.client.kv_client(),
            prefix: self.prefix.clone(),
        }
    }
}

impl KvClient {
    pub async fn get(&mut self, key: impl AsRef<str>) -> Result<GetResponse, EtcdClientError> {
        let key = key.as_ref();
        self.client
            .get(self.format_key(key), None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get key from etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    pub async fn put(
        &mut self,
        key: impl AsRef<str>,
        value: impl AsRef<[u8]>,
    ) -> Result<PutResponse, EtcdClientError> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.client
            .put(self.format_key(key), value, None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to put key to etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    fn format_key(&self, key: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, key).into_bytes()
    }
}

impl error_stack::Context for EtcdClientError {}

impl std::fmt::Display for EtcdClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "etcd client error")
    }
}
