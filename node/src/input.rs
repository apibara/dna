use apibara_core::application::pb;

/// Turn multiple input streams into a single one.
pub struct InputStream {
}

#[derive(Debug, thiserror::Error)]
pub enum InputStreamError {
}

pub type Result<T> = std::result::Result<T, InputStreamError>;

impl InputStream {
    pub fn new_from_pb(inputs: &[pb::InputStream]) -> Result<Self> {
        todo!()
    }
}