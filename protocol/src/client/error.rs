use std::fmt;

#[derive(Debug)]
pub struct StreamClientError;

impl error_stack::Context for StreamClientError {}

impl fmt::Display for StreamClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("failed to create metadata interceptor")
    }
}
