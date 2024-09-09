use apibara_etcd::{EtcdClient, KvClient};
use error_stack::{Result, ResultExt};

pub static OPTIONS_PREFIX_KEY: &str = "options/";
pub static CHAIN_SEGMENT_SIZE_KEY: &str = "options/chain_segment_size";

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
        let size = size.to_string();
        self.client
            .put(CHAIN_SEGMENT_SIZE_KEY, size.as_bytes())
            .await
            .change_context(OptionsStoreError)
            .attach_printable("failed to set chain segment size")?;

        Ok(())
    }

    pub async fn get_chain_segment_size(&mut self) -> Result<Option<usize>, OptionsStoreError> {
        let response = self
            .client
            .get(CHAIN_SEGMENT_SIZE_KEY)
            .await
            .change_context(OptionsStoreError)
            .attach_printable("failed to get chain segment size")?;

        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };

        let size = String::from_utf8(kv.value().to_vec())
            .change_context(OptionsStoreError)
            .attach_printable("failed to decode chain segment size")?;

        let size = size
            .parse::<usize>()
            .change_context(OptionsStoreError)
            .attach_printable("failed to parse chain segment size")
            .attach_printable_lazy(|| format!("chain segment size: {}", size))?;

        Ok(size.into())
    }
}

impl error_stack::Context for OptionsStoreError {}

impl std::fmt::Display for OptionsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "options store error")
    }
}
