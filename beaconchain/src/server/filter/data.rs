use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct BlockData {
    header: bool,
    validators: BTreeSet<u32>,
}

impl BlockData {
    pub fn require_header(&mut self, header: bool) {
        self.header = header;
    }

    pub fn is_empty(&self) -> bool {
        (!self.header) && self.validators.is_empty()
    }

    pub fn extend_validators(&mut self, validators: impl Iterator<Item = u32>) {
        self.validators.extend(validators);
    }

    pub fn validators(&self) -> impl Iterator<Item = &u32> {
        self.validators.iter()
    }
}
