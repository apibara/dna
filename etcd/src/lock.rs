use std::time::{Duration, Instant};

use error_stack::{Result, ResultExt};
use etcd_client::{LeaseKeeper, LockResponse};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::EtcdClientError;

#[derive(Debug)]
pub struct LockOptions {
    pub ttl: i64,
}

pub struct LockClient {
    pub(crate) client: etcd_client::Client,
    pub(crate) prefix: String,
    pub(crate) options: LockOptions,
}

pub struct Lock {
    inner: LockResponse,
    keeper: LeaseKeeper,
    lease_id: i64,
    min_refresh_interval: Duration,
    last_refresh: Instant,
}

impl LockClient {
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn lock(
        &mut self,
        key: impl AsRef<str>,
        ct: CancellationToken,
    ) -> Result<Option<Lock>, EtcdClientError> {
        let key = key.as_ref();

        let lease = self
            .client
            .lease_grant(self.options.ttl, None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to acquire lock")?;

        let lease_id = lease.id();

        let (mut keep_alive, _) = self
            .client
            .lease_keep_alive(lease_id)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to keep lease alive")?;

        let options = etcd_client::LockOptions::new().with_lease(lease_id);

        let min_refresh_interval = Duration::from_secs(self.options.ttl as u64 / 2);

        let mut refresh_interval = tokio::time::interval(min_refresh_interval);

        let lock_fut = self.client.lock(self.format_key(key), options.into());
        tokio::pin!(lock_fut);

        // Keep refreshing the lease while waiting for the lock.
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    return Ok(None);
                }
                _ = refresh_interval.tick() => {
                    keep_alive.keep_alive().await.change_context(EtcdClientError)?;
                }
                response = &mut lock_fut => {
                    let inner = response.change_context(EtcdClientError)
                        .attach_printable("failed to lock key")
                        .attach_printable_lazy(|| format!("key: {}", key))?;

                    let lock = Lock {
                        inner,
                        keeper: keep_alive,
                        lease_id,
                        min_refresh_interval,
                        last_refresh: Instant::now(),
                    };

                    return Ok(lock.into())
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn unlock(&mut self, lock: Lock) -> Result<(), EtcdClientError> {
        let lock = lock.inner;
        let key = lock.key().to_vec();
        self.client
            .unlock(key)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to unlock lock")?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn is_locked(&mut self, lock: &Lock) -> Result<bool, EtcdClientError> {
        let response = self
            .client
            .lease_time_to_live(lock.lease_id, None)
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to get lease time to live")
            .attach_printable_lazy(|| format!("lease id: {}", lock.lease_id))?;
        Ok(response.ttl() > 0)
    }

    fn format_key(&self, key: &str) -> Vec<u8> {
        format!("{}{}", self.prefix, key).into_bytes()
    }
}

impl Lock {
    pub async fn keep_alive(&mut self) -> Result<(), EtcdClientError> {
        if self.last_refresh.elapsed() <= self.min_refresh_interval {
            return Ok(());
        }

        debug!(lease_id = %self.lease_id, "send keep alive message");
        self.keeper
            .keep_alive()
            .await
            .change_context(EtcdClientError)
            .attach_printable("failed to keep lease alive")?;
        self.last_refresh = Instant::now();

        Ok(())
    }
}
impl Default for LockOptions {
    fn default() -> Self {
        Self { ttl: 60 }
    }
}
