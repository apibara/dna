use error_stack::{Result, ResultExt};
use etcd_client::WatchOptions;
use futures::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::client::EtcdClientError;

pub use etcd_client::{WatchResponse, Watcher};

#[derive(Clone)]
pub struct WatchClient {
    pub(crate) client: etcd_client::WatchClient,
    pub(crate) prefix: String,
}

impl WatchClient {
    pub async fn watch_prefix(
        &mut self,
        key: impl AsRef<str>,
        ct: CancellationToken,
    ) -> Result<
        (
            Watcher,
            impl Stream<Item = Result<WatchResponse, EtcdClientError>>,
        ),
        EtcdClientError,
    > {
        let key = key.as_ref();
        let options = WatchOptions::new().with_prefix();

        let (watcher, stream) = self
            .client
            .watch(self.format_key(key), options.into())
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to watch key with prefix from etcd")
            .attach_printable_lazy(|| format!("prefix: {}", key))?;

        let stream = stream
            .map(|res| res.change_context(EtcdClientError))
            .take_until(async move { ct.cancelled().await });

        Ok((watcher, stream))
    }

    fn format_key(&self, key: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, key).into_bytes()
    }
}
