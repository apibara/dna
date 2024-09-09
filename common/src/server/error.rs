#[derive(Debug)]
pub struct ServerError;

impl error_stack::Context for ServerError {}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error")
    }
}
