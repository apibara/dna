use std::{fs, path::PathBuf};

use apibara_dna_starknet::{ingestion::models, segment::store};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TestCase {
    pub comment: String,
    pub data: models::BlockWithReceipts,
}

#[test]
pub fn test_model_to_store_conversion() {
    let base_path = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/"));
    for file in fs::read_dir(base_path).unwrap() {
        let file = file.unwrap();
        let path = file.path();
        let content = fs::read_to_string(&path).unwrap();
        let test_case: TestCase = serde_json::from_str(&content).unwrap();
        let block = test_case.data;
        let stored_block = store::SingleBlock::from(block.clone());

        println!("{}", test_case.comment);

        assert_eq!(block.block_number, stored_block.header.block_number);
        assert_eq!(block.transactions.len(), stored_block.transactions.len());
        assert_eq!(block.transactions.len(), stored_block.receipts.len());
    }
}
