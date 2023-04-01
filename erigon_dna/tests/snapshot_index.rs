use std::path::PathBuf;

use erigon_dna::snapshot::index::Index;
use tracing_test::traced_test;

#[test]
#[traced_test]
fn test_index() {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-008630-008631-bodies.idx");

    let index = Index::new(dat).unwrap();
    let base_id = index.base_data_id();
    assert_eq!(base_id, 8_630_000);
    let value = index.ordinal_lookup(8_630_639 - base_id).unwrap();
    assert_eq!(value, 5842);
}

#[test]
#[traced_test]
fn test_index_elias_fano_overflow() {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-006000-006500-headers.idx");

    let index = Index::new(dat).unwrap();
    let base_id = index.base_data_id();
    assert_eq!(base_id, 6_000_000);
    let value = index.ordinal_lookup(6_016_639 - base_id).unwrap();
    assert_eq!(value, 4906730);
    let value = index.ordinal_lookup(6_016_640 - base_id).unwrap();
    assert_eq!(value, 4907104);
}
