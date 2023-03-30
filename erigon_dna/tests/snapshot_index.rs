use std::path::PathBuf;

use erigon_dna::snapshot::index::Index;
use tracing_test::traced_test;

#[test]
#[traced_test]
fn test_index() {
    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-008630-008631-bodies.idx");

    let index = Index::new(dat).unwrap();
    todo!()
}
