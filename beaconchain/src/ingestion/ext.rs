use apibara_dna_common::core::Cursor;

use super::provider::models;

pub trait BeaconResponseExt {
    fn cursor(&self) -> Cursor;
}

impl BeaconResponseExt for models::HeaderResponse {
    fn cursor(&self) -> Cursor {
        let block_number = self.data.header.message.slot;
        let block_hash = self.data.root;
        Cursor::new(block_number, block_hash.to_vec())
    }
}
