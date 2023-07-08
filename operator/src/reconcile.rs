use kube::{
    runtime::{
        controller::{Action, Error as ControllerError},
        reflector::ObjectRef,
        watcher::Error as WatcherError,
    },
    Client,
};

use crate::configuration::Configuration;

#[derive(Clone)]
pub struct Context {
    /// Kube client.
    pub client: Client,
    /// Operator configuration.
    pub configuration: Configuration,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("CRD not installed: {0}")]
    CrdNotInstalled(String),
    #[error("Finalizer error: {0}")]
    Finalizer(#[source] Box<kube::runtime::finalizer::Error<Error>>),
    #[error("Kube error: {0}")]
    Kube(#[from] kube::Error),
}

pub type ReconcileItem<K> = Result<(ObjectRef<K>, Action), ControllerError<Error, WatcherError>>;
