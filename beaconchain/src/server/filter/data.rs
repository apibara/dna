#[derive(Debug, Default, Clone)]
pub struct BlockData {
    header: bool,
}

impl BlockData {
    pub fn require_header(&mut self, header: bool) {
        self.header = header;
    }

    pub fn is_empty(&self) -> bool {
        !self.header
    }
}
