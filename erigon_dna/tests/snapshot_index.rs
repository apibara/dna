use std::path::PathBuf;

use erigon_dna::snapshot::recsplit::Index;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use tracing_test::traced_test;

#[test]
// #[traced_test]
fn test_index() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let mut dat = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dat.push("tests/data/v1-008630-008631-bodies.idx");

    let index = Index::new(dat).unwrap();
    todo!()
}
