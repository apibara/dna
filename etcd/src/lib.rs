//! A collection of utilities for working with etcd.
mod client;
mod kv;
mod lock;
mod utils;
mod watch;

pub use self::client::{
    AuthOptions, EtcdClient, EtcdClientError, EtcdClientOptions, StatusResponse,
};
pub use self::kv::{GetResponse, KvClient, PutResponse};
pub use self::lock::{Lock, LockClient, LockOptions};
pub use self::utils::normalize_prefix;
pub use self::watch::WatchClient;

pub use etcd_client::LeaseKeepAliveStream;
