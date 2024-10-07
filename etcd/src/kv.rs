use error_stack::{Result, ResultExt};

use etcd_client::GetOptions;
pub use etcd_client::{GetResponse, PutResponse};

use crate::client::EtcdClientError;

#[derive(Clone)]
pub struct KvClient {
    pub(crate) client: etcd_client::KvClient,
    pub(crate) prefix: String,
}

impl KvClient {
    #[tracing::instrument(level = "debug", skip_all, fields(key = prefix.as_ref()))]
    pub async fn get_prefix(
        &mut self,
        prefix: impl AsRef<str>,
    ) -> Result<GetResponse, EtcdClientError> {
        let prefix = prefix.as_ref();
        let options = GetOptions::new().with_prefix();
        self.client
            .get(self.format_key(prefix), options.into())
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get key with prefix from etcd")
            .attach_printable_lazy(|| format!("prefix: {}", prefix))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
    pub async fn get(&mut self, key: impl AsRef<str>) -> Result<GetResponse, EtcdClientError> {
        let key = key.as_ref();
        self.client
            .get(self.format_key(key), None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get key from etcd")
            .attach_printable_lazy(|| format!("key: {}", key))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(key = key.as_ref()))]
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
