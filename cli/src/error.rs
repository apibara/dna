use std::fmt;

#[derive(Debug)]
pub struct CliError;
impl error_stack::Context for CliError {}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("cli operation failed")
    }
}
