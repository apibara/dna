use error_stack::{Result, ResultExt};

use crate::{lock::LockOptions, utils::normalize_prefix, watch::WatchClient};

pub use etcd_client::StatusResponse;

use crate::{kv::KvClient, lock::LockClient};

#[derive(Debug)]
pub struct EtcdClientError;

#[derive(Debug, Default)]
pub struct AuthOptions {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Default)]
pub struct EtcdClientOptions {
    pub prefix: Option<String>,
    pub auth: Option<AuthOptions>,
}

#[derive(Clone)]
pub struct EtcdClient {
    pub(crate) client: etcd_client::Client,
    prefix: String,
}

impl EtcdClient {
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: EtcdClientOptions,
    ) -> Result<Self, EtcdClientError> {
        let connect_options = if let Some(auth) = options.auth {
            etcd_client::ConnectOptions::new()
                .with_user(auth.user, auth.password)
                .into()
        } else {
            None
        };

        let client = etcd_client::Client::connect(endpoints, connect_options)
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

    pub fn watch_client(&self) -> WatchClient {
        WatchClient {
            client: self.client.watch_client(),
            prefix: self.prefix.clone(),
        }
    }

    pub fn lock_client(&self, options: LockOptions) -> LockClient {
        LockClient {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            options,
        }
    }
}

impl error_stack::Context for EtcdClientError {}

impl std::fmt::Display for EtcdClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "etcd client error")
    }
}
