use apibara_dna_common::query::{BlockFilter, Filter};

pub trait BlockFilterExt {
    fn compile_to_block_filter(&self) -> tonic::Result<BlockFilter, tonic::Status>;
}

pub trait FragmentFilterExt {
    fn compile_to_filter(&self) -> tonic::Result<Filter, tonic::Status>;
}
