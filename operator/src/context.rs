use kube::Client;

use crate::configuration::Configuration;

#[derive(Clone)]
pub struct Context {
    /// Kube client.
    pub client: Client,
    /// Operator configuration.
    pub configuration: Configuration,
}

#[derive(thiserror::Error, Debug)]
pub enum OperatorError {
    #[error("K8s api error: {0}")]
    Kube(#[from] kube::Error),
    #[error("K8s finalizer error: {0}")]
    Finalizer(#[source] Box<kube::runtime::finalizer::Error<OperatorError>>),
    #[error("CRD not installed: {0}")]
    CrdNotInstalled(String),
    #[error("CRD is namespaced")]
    CrdInNamespaced,
}
