use std::fmt;

use kube::Client;

use crate::configuration::Configuration;

#[derive(Clone)]
pub struct Context {
    /// Kube client.
    pub client: Client,
    /// Operator configuration.
    pub configuration: Configuration,
}

#[derive(Debug)]
pub struct OperatorError;
impl error_stack::Context for OperatorError {}

impl fmt::Display for OperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operator operation failed")
    }
}
