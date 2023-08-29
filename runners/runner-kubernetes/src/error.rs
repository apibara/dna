use std::fmt;

#[derive(Debug)]
pub struct KubeRunnerError;
impl error_stack::Context for KubeRunnerError {}

impl fmt::Display for KubeRunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("kube runner operation failed")
    }
}
