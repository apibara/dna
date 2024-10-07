use apibara_etcd::{EtcdClient, KvClient};
use error_stack::{Result, ResultExt};

pub static OPTIONS_PREFIX_KEY: &str = "options/";
pub static CHAIN_SEGMENT_SIZE_KEY: &str = "options/chain_segment_size";
pub static SEGMENT_SIZE_KEY: &str = "options/segment_size";
pub static GROUP_SIZE_KEY: &str = "options/group_size";

#[derive(Debug)]
pub struct OptionsStoreError;

/// A client to get and set DNA options.
pub struct OptionsStore {
    client: KvClient,
}

impl OptionsStore {
    pub fn new(client: &EtcdClient) -> Self {
        let client = client.kv_client();
        Self { client }
    }

    pub async fn set_chain_segment_size(&mut self, size: usize) -> Result<(), OptionsStoreError> {
        self.set_usize(CHAIN_SEGMENT_SIZE_KEY, size)
            .await
            .attach_printable("failed to set chain segment size")
    }

    pub async fn set_segment_size(&mut self, size: usize) -> Result<(), OptionsStoreError> {
        self.set_usize(SEGMENT_SIZE_KEY, size)
            .await
            .attach_printable("failed to set segment size")
    }

    pub async fn set_group_size(&mut self, size: usize) -> Result<(), OptionsStoreError> {
        self.set_usize(GROUP_SIZE_KEY, size)
            .await
            .attach_printable("failed to set group size")
    }

    pub async fn get_chain_segment_size(&mut self) -> Result<Option<usize>, OptionsStoreError> {
        self.get_usize(CHAIN_SEGMENT_SIZE_KEY)
            .await
            .attach_printable("failed to get chain segment size")
    }

    pub async fn get_segment_size(&mut self) -> Result<Option<usize>, OptionsStoreError> {
        self.get_usize(SEGMENT_SIZE_KEY)
            .await
            .attach_printable("failed to get segment size")
    }

    pub async fn get_group_size(&mut self) -> Result<Option<usize>, OptionsStoreError> {
        self.get_usize(GROUP_SIZE_KEY)
            .await
            .attach_printable("failed to get group size")
    }

    async fn set_usize(&mut self, key: &str, size: usize) -> Result<(), OptionsStoreError> {
        let size = size.to_string();
        self.client
            .put(key, size.as_bytes())
            .await
            .change_context(OptionsStoreError)
            .attach_printable("failed to set size")?;

        Ok(())
    }

    async fn get_usize(&mut self, key: &str) -> Result<Option<usize>, OptionsStoreError> {
        let response = self
            .client
            .get(key)
            .await
            .change_context(OptionsStoreError)
            .attach_printable("failed to get size options")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let size = String::from_utf8(kv.value().to_vec())
            .change_context(OptionsStoreError)
            .attach_printable("failed to decode size")?;

        let size = size
            .parse::<usize>()
            .change_context(OptionsStoreError)
            .attach_printable("failed to parse size")
            .attach_printable_lazy(|| format!("size: {}", size))?;

        Ok(size.into())
    }
}

impl error_stack::Context for OptionsStoreError {}

impl std::fmt::Display for OptionsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "options store error")
    }
}
