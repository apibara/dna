#[derive(Debug)]
pub struct DebugCommandError;

impl error_stack::Context for DebugCommandError {}

impl std::fmt::Display for DebugCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "debug command error")
    }
}
