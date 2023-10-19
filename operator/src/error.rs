use std::fmt;

#[derive(Debug)]
pub struct OperatorError;
impl error_stack::Context for OperatorError {}

impl fmt::Display for OperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operator operation failed")
    }
}
